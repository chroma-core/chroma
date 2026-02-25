use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider, BlockfileFlusher,
    BlockfileReader, BlockfileWriterOptions,
};
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{
    default_center_drift_threshold, default_construction_ef_spann, default_m_spann,
    default_merge_threshold, default_nreplica_count, default_reassign_neighbor_count,
    default_search_ef_spann, default_split_threshold, default_write_nprobe,
    default_write_rng_epsilon, default_write_rng_factor, Cmek, CollectionUuid, DataRecord,
    QuantizedCluster, SpannIndexConfig,
};
use dashmap::{DashMap, DashSet};
use faer::{
    col::ColRef,
    stats::{
        prelude::{Distribution, StandardNormal, ThreadRng},
        UnitaryMat,
    },
    Mat,
};
use simsimd::SpatialSimilarity;
use thiserror::Error;
use tracing::Instrument;
use uuid::Uuid;

use crate::{
    quantization::Code,
    spann::utils,
    usearch::{USearchIndex, USearchIndexConfig, USearchIndexProvider},
    IndexUuid, OpenMode, SearchResult, VectorIndex, VectorIndexProvider,
};

// =============================================================================
// Statistics
// =============================================================================

pub mod stats {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    /// Statistics for a single method.
    #[derive(Default)]
    pub struct MethodStats {
        pub calls: AtomicU64,
        pub total_nanos: AtomicU64,
    }

    impl MethodStats {
        #[inline]
        pub fn record(&self, nanos: u64) {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.total_nanos.fetch_add(nanos, Ordering::Relaxed);
        }

        pub fn get(&self) -> (u64, u64) {
            (
                self.calls.load(Ordering::Relaxed),
                self.total_nanos.load(Ordering::Relaxed),
            )
        }
    }

    /// Snapshot of stats for a single method (non-atomic, owned).
    #[derive(Clone, Copy, Default)]
    pub struct MethodSnapshot {
        pub calls: u64,
        pub total_nanos: u64,
    }

    impl MethodSnapshot {
        pub fn avg_nanos(&self) -> Option<u64> {
            if self.calls > 0 {
                Some(self.total_nanos / self.calls)
            } else {
                None
            }
        }
    }

    /// Statistics for cluster size distribution.
    #[derive(Clone, Default)]
    pub struct ClusterSizeStats {
        pub num_centroids: u64,
        pub min: u64,
        pub max: u64,
        pub median: u64,
        pub p90: u64,
        pub p99: u64,
        pub avg: f64,
        pub std: f64,
    }

    impl ClusterSizeStats {
        /// Compute cluster size statistics from a slice of sizes.
        pub fn from_sizes(sizes: &[usize]) -> Self {
            if sizes.is_empty() {
                return Self::default();
            }

            let mut sorted: Vec<usize> = sizes.to_vec();
            sorted.sort_unstable();

            let n = sorted.len();
            let sum: usize = sorted.iter().sum();
            let avg = sum as f64 / n as f64;

            let variance = sorted
                .iter()
                .map(|&x| (x as f64 - avg).powi(2))
                .sum::<f64>()
                / n as f64;
            let std = variance.sqrt();

            let percentile = |p: f64| -> u64 {
                let idx = ((n as f64 - 1.0) * p).round() as usize;
                sorted[idx.min(n - 1)] as u64
            };

            Self {
                num_centroids: n as u64,
                min: sorted[0] as u64,
                max: sorted[n - 1] as u64,
                median: percentile(0.5),
                p90: percentile(0.9),
                p99: percentile(0.99),
                avg,
                std,
            }
        }
    }

    /// Snapshot of all method stats (non-atomic, owned).
    #[derive(Clone, Default)]
    pub struct StatsSnapshot {
        pub add: MethodSnapshot,
        pub navigate: MethodSnapshot,
        pub register: MethodSnapshot,
        pub spawn: MethodSnapshot,
        pub scrub: MethodSnapshot,
        pub split: MethodSnapshot,
        pub merge: MethodSnapshot,
        pub reassign: MethodSnapshot,
        pub drop: MethodSnapshot,
        pub load: MethodSnapshot,
        pub load_raw: MethodSnapshot,
        pub load_raw_points: u64,
        pub cluster_stats: ClusterSizeStats,
    }

    impl StatsSnapshot {
        pub fn get(&self, name: &str) -> MethodSnapshot {
            match name {
                "add" => self.add,
                "navigate" => self.navigate,
                "register" => self.register,
                "spawn" => self.spawn,
                "scrub" => self.scrub,
                "split" => self.split,
                "merge" => self.merge,
                "reassign" => self.reassign,
                "drop" => self.drop,
                "load" => self.load,
                "load_raw" => self.load_raw,
                _ => MethodSnapshot::default(),
            }
        }
    }

    /// Aggregated statistics for all instrumented methods.
    #[derive(Default)]
    pub struct QuantizedSpannStats {
        pub add: MethodStats,
        pub navigate: MethodStats,
        pub register: MethodStats,
        pub spawn: MethodStats,
        pub scrub: MethodStats,
        pub split: MethodStats,
        pub merge: MethodStats,
        pub reassign: MethodStats,
        pub drop: MethodStats,
        pub load: MethodStats,
        pub load_raw: MethodStats,
        pub load_raw_points: AtomicU64,
    }

    impl QuantizedSpannStats {
        pub fn snapshot(&self, cluster_sizes: &[usize]) -> StatsSnapshot {
            let snap = |m: &MethodStats| {
                let (calls, total_nanos) = m.get();
                MethodSnapshot { calls, total_nanos }
            };
            StatsSnapshot {
                add: snap(&self.add),
                navigate: snap(&self.navigate),
                register: snap(&self.register),
                spawn: snap(&self.spawn),
                scrub: snap(&self.scrub),
                split: snap(&self.split),
                merge: snap(&self.merge),
                reassign: snap(&self.reassign),
                drop: snap(&self.drop),
                load: snap(&self.load),
                load_raw: snap(&self.load_raw),
                load_raw_points: self.load_raw_points.load(Ordering::Relaxed),
                cluster_stats: ClusterSizeStats::from_sizes(cluster_sizes),
            }
        }
    }

    // All methods ordered by path: Write Path, Balance Path, I/O
    const ALL_METHODS: &[&str] = &[
        "add", "navigate", "register", "spawn", // Write Path
        "scrub", "split", "merge", "reassign", "drop", // Balance Path
        "load", "load_raw", // I/O
    ];

    fn format_duration(nanos: u64) -> String {
        if nanos < 1_000 {
            format!("{}ns", nanos)
        } else if nanos < 1_000_000 {
            format!("{:.1}µs", nanos as f64 / 1_000.0)
        } else if nanos < 1_000_000_000 {
            format!("{:.2}ms", nanos as f64 / 1_000_000.0)
        } else {
            format!("{:.2}s", nanos as f64 / 1_000_000_000.0)
        }
    }

    fn format_count(n: u64) -> String {
        if n < 1_000 {
            n.to_string()
        } else if n < 1_000_000 {
            format!("{:.1}K", n as f64 / 1_000.0)
        } else {
            format!("{:.2}M", n as f64 / 1_000_000.0)
        }
    }

    fn format_cluster_stats_table(snapshots: &[StatsSnapshot]) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        writeln!(out, "\n=== Cluster Statistics ===").unwrap();
        writeln!(
            out,
            "| CP | Centroids |   Min |   Max | Median |   P90 |   P99 |    Avg |    Std |"
        )
        .unwrap();
        writeln!(
            out,
            "|----|-----------|-------|-------|--------|-------|-------|--------|--------|"
        )
        .unwrap();
        for (i, snap) in snapshots.iter().enumerate() {
            let cs = &snap.cluster_stats;
            writeln!(
                out,
                "| {:>2} | {:>9} | {:>5} | {:>5} | {:>6} | {:>5} | {:>5} | {:>6.1} | {:>6.1} |",
                i + 1,
                format_count(cs.num_centroids),
                cs.min,
                cs.max,
                cs.median,
                cs.p90,
                cs.p99,
                cs.avg,
                cs.std
            )
            .unwrap();
        }
        out
    }

    fn format_task_counts_table(snapshots: &[StatsSnapshot]) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        writeln!(out, "\n=== Task Counts ===").unwrap();
        // Header
        write!(out, "| CP |").unwrap();
        for method in ALL_METHODS {
            write!(out, " {:>8} |", method).unwrap();
        }
        writeln!(out).unwrap();
        // Separator
        write!(out, "|----|").unwrap();
        for _ in ALL_METHODS {
            write!(out, "----------|").unwrap();
        }
        writeln!(out).unwrap();
        // Data rows
        for (i, snap) in snapshots.iter().enumerate() {
            write!(out, "| {:>2} |", i + 1).unwrap();
            for method in ALL_METHODS {
                let m = snap.get(method);
                write!(out, " {:>8} |", format_count(m.calls)).unwrap();
            }
            writeln!(out).unwrap();
        }
        out
    }

    fn format_task_timing_table(snapshots: &[StatsSnapshot]) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        writeln!(out, "\n=== Task Total Time ===").unwrap();
        // Header
        write!(out, "| CP |").unwrap();
        for method in ALL_METHODS {
            write!(out, " {:>8} |", method).unwrap();
        }
        write!(out, " raw_pts |  raw/pt |").unwrap();
        writeln!(out).unwrap();
        // Separator
        write!(out, "|----|").unwrap();
        for _ in ALL_METHODS {
            write!(out, "----------|").unwrap();
        }
        write!(out, "---------|---------|").unwrap();
        writeln!(out).unwrap();
        // Data rows
        for (i, snap) in snapshots.iter().enumerate() {
            write!(out, "| {:>2} |", i + 1).unwrap();
            for method in ALL_METHODS {
                let m = snap.get(method);
                write!(out, " {:>8} |", format_duration(m.total_nanos)).unwrap();
            }
            // load_raw points and avg per point
            let points = snap.load_raw_points;
            let avg_per_point = if points > 0 {
                format_duration(snap.load_raw.total_nanos / points)
            } else {
                "-".to_string()
            };
            write!(out, " {:>7} | {:>7} |", format_count(points), avg_per_point).unwrap();
            writeln!(out).unwrap();
        }
        out
    }

    fn format_task_avg_time_table(snapshots: &[StatsSnapshot]) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        writeln!(out, "\n=== Task Avg Time ===").unwrap();
        // Header
        write!(out, "| CP |").unwrap();
        for method in ALL_METHODS {
            write!(out, " {:>8} |", method).unwrap();
        }
        writeln!(out).unwrap();
        // Separator
        write!(out, "|----|").unwrap();
        for _ in ALL_METHODS {
            write!(out, "----------|").unwrap();
        }
        writeln!(out).unwrap();
        // Data rows
        for (i, snap) in snapshots.iter().enumerate() {
            write!(out, "| {:>2} |", i + 1).unwrap();
            for method in ALL_METHODS {
                let m = snap.get(method);
                let avg = if m.calls > 0 {
                    format_duration(m.total_nanos / m.calls)
                } else {
                    "-".to_string()
                };
                write!(out, " {:>8} |", avg).unwrap();
            }
            writeln!(out).unwrap();
        }
        out
    }

    /// Format all batch stats as summary tables.
    pub fn format_batch_tables(snapshots: &[StatsSnapshot]) -> String {
        let mut out = String::new();
        out.push_str(&format_cluster_stats_table(snapshots));
        out.push_str(&format_task_counts_table(snapshots));
        out.push_str(&format_task_timing_table(snapshots));
        out.push_str(&format_task_avg_time_table(snapshots));
        out
    }

    /// RAII guard for timing a method.
    pub struct TimedGuard<'a> {
        stats: &'a MethodStats,
        start: Instant,
    }

    impl<'a> TimedGuard<'a> {
        #[inline]
        pub fn new(stats: &'a MethodStats) -> Self {
            Self {
                stats,
                start: Instant::now(),
            }
        }
    }

    impl Drop for TimedGuard<'_> {
        #[inline]
        fn drop(&mut self) {
            self.stats.record(self.start.elapsed().as_nanos() as u64);
        }
    }
}

pub use stats::{format_batch_tables, ClusterSizeStats, QuantizedSpannStats, StatsSnapshot};

// Blockfile prefixes
pub const PREFIX_CENTER: &str = "center";
const PREFIX_LENGTH: &str = "length";
const PREFIX_NEXT_CLUSTER: &str = "next";
pub const PREFIX_ROTATION: &str = "rotation";
pub const PREFIX_VERSION: &str = "version";

// Key for singleton values (center, next_cluster_id)
pub const SINGLETON_KEY: u32 = 0;

/// Maximum recursion depth for balance → split/merge → reassign → balance chains.
/// Beyond this depth, oversized clusters are deferred to the next top-level insert.
const MAX_BALANCE_DEPTH: u32 = 4;

/// In-memory staging for a quantized cluster head.
#[derive(Clone)]
struct QuantizedDelta {
    center: Arc<[f32]>,
    codes: Vec<Arc<[u8]>>,
    ids: Vec<u32>,
    length: usize,
    versions: Vec<u32>,
}

#[derive(Clone, Debug)]
pub struct QuantizedSpannIds {
    pub embedding_metadata_id: Uuid,
    pub prefix_path: String,
    pub quantized_centroid_id: IndexUuid,
    pub quantized_cluster_id: Uuid,
    pub raw_centroid_id: IndexUuid,
    pub scalar_metadata_id: Uuid,
}

#[derive(Error, Debug)]
pub enum QuantizedSpannError {
    #[error("Centroid index error: {0}")]
    CentroidIndex(Box<dyn ChromaError>),
    #[error("Blockfile error: {0}")]
    Blockfile(Box<dyn ChromaError>),
    #[error("Dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
}

impl ChromaError for QuantizedSpannError {
    fn code(&self) -> ErrorCodes {
        match self {
            QuantizedSpannError::CentroidIndex(err) => err.code(),
            QuantizedSpannError::Blockfile(err) => err.code(),
            QuantizedSpannError::DimensionMismatch { .. } => ErrorCodes::InvalidArgument,
        }
    }
}

/// Mutable quantized SPANN index, generic over centroid index.
#[derive(Clone)]
pub struct QuantizedSpannIndexWriter<I: VectorIndex> {
    // === Config ===
    cluster_block_size: usize,
    cmek: Option<Cmek>,
    collection_id: CollectionUuid,
    config: SpannIndexConfig,
    dimension: usize,
    distance_function: DistanceFunction,
    file_ids: Option<QuantizedSpannIds>,
    prefix_path: String,

    // === Centroid Index ===
    next_cluster_id: Arc<AtomicU32>,
    quantized_centroid: I,
    raw_centroid: I,

    // === Quantization ===
    center: Arc<[f32]>,
    rotation: Mat<f32>,

    // === In-Memory State ===
    // This contains incremental changes for the quantized clusters.
    cluster_deltas: Arc<DashMap<u32, QuantizedDelta>>,
    embeddings: Arc<DashMap<u32, Arc<[f32]>>>,
    tombstones: Arc<DashSet<u32>>,
    versions: Arc<DashMap<u32, u32>>,

    // === Blockfile Readers ===
    quantized_cluster_reader: Option<BlockfileReader<'static, u32, QuantizedCluster<'static>>>,
    // NOTE(sicheng): This is the record segment's id_to_data blockfile reader.
    // This is a temporary solution for loading raw embeddings; a dedicated
    // raw embedding store may be introduced in the future.
    raw_embedding_reader: Option<BlockfileReader<'static, u32, DataRecord<'static>>>,

    // === Dedup Sets ===
    // This contains the set of cluster ids in the balance (scrub/split/merge) routine.
    // It is used to prevent concurrent balancing attempts on the same clusters.
    balancing: Arc<DashSet<u32>>,

    // === Statistics ===
    stats: Arc<QuantizedSpannStats>,
}

impl<I: VectorIndex> QuantizedSpannIndexWriter<I> {
    pub async fn add(&self, id: u32, embedding: &[f32]) -> Result<(), QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.add);
        if embedding.len() != self.dimension {
            return Err(QuantizedSpannError::DimensionMismatch {
                expected: self.dimension,
                got: embedding.len(),
            });
        }
        let rotated = self.rotate(embedding);
        self.embeddings.insert(id, rotated.clone());
        self.insert(id, rotated).await
    }

    pub fn remove(&self, id: u32) {
        self.upgrade_version(id);
    }

    /// Get the statistics for this index.
    pub fn stats(&self) -> &QuantizedSpannStats {
        &self.stats
    }

    /// Get current cluster sizes.
    pub fn cluster_sizes(&self) -> Vec<usize> {
        self.cluster_deltas
            .iter()
            .map(|entry| entry.value().length)
            .collect()
    }

    /// Search for the k nearest neighbors of a query vector.
    pub async fn search(
        &self,
        k: usize,
        query: &[f32],
        nprobe: usize,
    ) -> Result<SearchResult, QuantizedSpannError> {
        use std::collections::HashSet;

        let rotated = self.rotate(query);

        // Navigate: find nearest clusters using quantized centroid
        let cluster_ids = self
            .quantized_centroid
            .search(&rotated, nprobe)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?
            .keys;

        // Scan clusters and collect results
        let mut measured = HashSet::new();
        let mut results = Vec::new();

        let q_norm = (f32::dot(&rotated, &rotated).unwrap_or(0.0) as f32).sqrt();

        for cluster_id in cluster_ids {
            self.load(cluster_id).await?;

            let Some(delta) = self.cluster_deltas.get(&cluster_id) else {
                continue;
            };

            let center = &delta.center;
            let c_norm = (f32::dot(center, center).unwrap_or(0.0) as f32).sqrt();
            let c_dot_q = f32::dot(center, &rotated).unwrap_or(0.0) as f32;
            let r_q: Vec<f32> = rotated
                .iter()
                .zip(center.iter())
                .map(|(q, c)| q - c)
                .collect();

            for (i, (id, version)) in delta.ids.iter().zip(delta.versions.iter()).enumerate() {
                if !self.is_valid(*id, *version) || !measured.insert(*id) {
                    continue;
                }

                let code = Code::<&[u8]>::new(&delta.codes[i]);
                let distance =
                    code.distance_query(&self.distance_function, &r_q, c_norm, c_dot_q, q_norm);
                results.push((*id, distance));
            }
        }

        // Sort by distance ascending and truncate to k
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);

        // Convert to SearchResult
        let (keys, distances): (Vec<u32>, Vec<f32>) = results.into_iter().unzip();
        Ok(SearchResult { keys, distances })
    }
}

impl<I: VectorIndex> QuantizedSpannIndexWriter<I> {
    /// Append a point to an existing cluster. Returns new length, or None if cluster not found.
    fn append(&self, cluster_id: u32, id: u32, version: u32, code: Arc<[u8]>) -> Option<usize> {
        let mut delta = self.cluster_deltas.get_mut(&cluster_id)?;
        delta.codes.push(code);
        delta.ids.push(id);
        delta.length += 1;
        delta.versions.push(version);
        Some(delta.length)
    }

    /// Balance a cluster: scrub then trigger split/merge if needed.
    /// `depth` tracks recursion depth through balance → split/merge → reassign → balance chains.
    async fn balance(&self, cluster_id: u32, depth: u32) -> Result<(), QuantizedSpannError> {
        if !self.balancing.insert(cluster_id) {
            return Ok(());
        }

        let Some(len) = self.scrub(cluster_id).await? else {
            self.balancing.remove(&cluster_id);
            return Ok(());
        };

        let split_threshold = self
            .config
            .split_threshold
            .unwrap_or(default_split_threshold()) as usize;
        let merge_threshold = self
            .config
            .merge_threshold
            .unwrap_or(default_merge_threshold()) as usize;

        if len > split_threshold {
            self.split(cluster_id, depth).await?;
        } else if len > 0 && len < merge_threshold {
            self.merge(cluster_id, depth).await?;
        }

        self.balancing.remove(&cluster_id);
        Ok(())
    }

    /// Get the centroid for a cluster, cloning to release the lock.
    fn centroid(&self, cluster_id: u32) -> Option<Arc<[f32]>> {
        self.cluster_deltas
            .get(&cluster_id)
            .map(|delta| delta.center.clone())
    }

    /// Compute distance between two vectors using the configured distance function.
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        self.distance_function.distance(a, b)
    }

    /// Remove a cluster from both centroid indexes and register as tombstone.
    /// Load raw embeddings and returns the delta if the cluster existed.
    async fn drop(&self, cluster_id: u32) -> Result<Option<QuantizedDelta>, QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.drop);
        self.raw_centroid
            .remove(cluster_id)
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
        self.quantized_centroid
            .remove(cluster_id)
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
        self.tombstones.insert(cluster_id);

        let Some((_, delta)) = self.cluster_deltas.remove(&cluster_id) else {
            return Ok(None);
        };

        let ids = delta
            .ids
            .iter()
            .zip(delta.versions.iter())
            .filter_map(|(id, version)| self.is_valid(*id, *version).then_some(*id))
            .collect::<Vec<_>>();
        self.load_raw(&ids).await?;

        Ok(Some(delta))
    }

    /// Insert a rotated vector into the index.
    async fn insert(&self, id: u32, embedding: Arc<[f32]>) -> Result<(), QuantizedSpannError> {
        let write_nprobe = self.config.write_nprobe.unwrap_or(default_write_nprobe()) as usize;
        let candidates = self.navigate(&embedding, write_nprobe)?;
        let rng_cluster_ids = self.rng_select(&candidates).keys;

        for cluster_id in self.register(id, embedding, &rng_cluster_ids)? {
            Box::pin(self.balance(cluster_id, 0)).await?;
        }

        Ok(())
    }

    /// Check if a point is valid (version matches current version).
    fn is_valid(&self, id: u32, version: u32) -> bool {
        self.versions
            .get(&id)
            .is_some_and(|global_version| *global_version == version)
    }

    /// Load cluster data from reader into deltas.
    async fn load(&self, cluster_id: u32) -> Result<(), QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.load);
        let Some(reader) = &self.quantized_cluster_reader else {
            return Ok(());
        };

        if self
            .cluster_deltas
            .get(&cluster_id)
            .is_none_or(|delta| delta.ids.len() >= delta.length)
        {
            return Ok(());
        }

        let Some(persisted) = reader
            .get("", cluster_id)
            .await
            .map_err(QuantizedSpannError::Blockfile)?
        else {
            return Ok(());
        };

        let code_size = Code::<&[u8]>::size(self.dimension);
        if let Some(mut delta) = self.cluster_deltas.get_mut(&cluster_id) {
            if delta.ids.len() < delta.length {
                for ((id, version), code) in persisted
                    .ids
                    .iter()
                    .zip(persisted.versions.iter())
                    .zip(persisted.codes.chunks(code_size))
                {
                    delta.codes.push(Arc::from(code));
                    delta.ids.push(*id);
                    delta.versions.push(*version);
                }
            }
        }

        Ok(())
    }

    /// Load raw embeddings for given ids into the embeddings cache.
    async fn load_raw(&self, ids: &[u32]) -> Result<(), QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.load_raw);
        let Some(reader) = &self.raw_embedding_reader else {
            return Ok(());
        };

        let missing_ids = ids
            .iter()
            .copied()
            .filter(|id| !self.embeddings.contains_key(id))
            .collect::<Vec<_>>();

        reader
            .load_data_for_keys(missing_ids.iter().map(|id| (String::new(), *id)))
            .await;

        let num_loaded = missing_ids.len();
        for id in missing_ids {
            if let Some(record) = reader
                .get("", id)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
            {
                self.embeddings.insert(id, self.rotate(record.embedding));
            }
        }

        self.stats
            .load_raw_points
            .fetch_add(num_loaded as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Merge a small cluster into a nearby cluster.
    async fn merge(&self, cluster_id: u32, depth: u32) -> Result<(), QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.merge);
        if depth > MAX_BALANCE_DEPTH {
            return Ok(());
        }

        let Some(source_center) = self.centroid(cluster_id) else {
            return Ok(());
        };

        let write_nprobe = self.config.write_nprobe.unwrap_or(default_write_nprobe()) as usize;
        let neighbors = self.navigate(&source_center, write_nprobe)?;
        let Some(nearest_cluster_id) = neighbors
            .keys
            .iter()
            .copied()
            .find(|neighbor_cluster_id| *neighbor_cluster_id != cluster_id)
        else {
            return Ok(());
        };

        let Some(target_center) = self.centroid(nearest_cluster_id) else {
            return Ok(());
        };

        let Some(delta) = self.drop(cluster_id).await? else {
            return Ok(());
        };

        for (id, version) in delta.ids.iter().zip(delta.versions.iter()) {
            let Some(embedding) = self.embeddings.get(id).map(|emb| emb.clone()) else {
                continue;
            };

            let dist_to_target = self.distance(&embedding, &target_center);
            let dist_to_source = self.distance(&embedding, &source_center);

            if dist_to_target <= dist_to_source {
                let code = Code::<Vec<u8>>::quantize(&embedding, &target_center)
                    .as_ref()
                    .into();
                if self
                    .append(nearest_cluster_id, *id, *version, code)
                    .is_none()
                {
                    self.reassign(cluster_id, *id, *version, embedding, depth)
                        .await?;
                };
            } else {
                self.reassign(cluster_id, *id, *version, embedding, depth)
                    .await?;
            }
        }

        Ok(())
    }

    /// Query the centroid index for the nearest cluster heads.
    fn navigate(&self, query: &[f32], count: usize) -> Result<SearchResult, QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.navigate);
        self.raw_centroid
            .search(query, count)
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))
    }

    /// Reassign a vector to new clusters via RNG query.
    async fn reassign(
        &self,
        from_cluster_id: u32,
        id: u32,
        version: u32,
        embedding: Arc<[f32]>,
        depth: u32,
    ) -> Result<(), QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.reassign);
        if !self.is_valid(id, version) {
            return Ok(());
        }

        let write_nprobe = self.config.write_nprobe.unwrap_or(default_write_nprobe()) as usize;
        let candidates = self.navigate(&embedding, write_nprobe)?;
        let rng_cluster_ids = self.rng_select(&candidates).keys;

        if rng_cluster_ids.contains(&from_cluster_id) {
            return Ok(());
        }

        if !self.is_valid(id, version) {
            return Ok(());
        }

        for cluster_id in self.register(id, embedding, &rng_cluster_ids)? {
            Box::pin(self.balance(cluster_id, depth + 1)).await?;
        }

        Ok(())
    }

    /// Register a vector in target clusters.
    /// Returns the clusters whose lengths exceed split threshold
    fn register(
        &self,
        id: u32,
        embedding: Arc<[f32]>,
        target_cluster_ids: &[u32],
    ) -> Result<Vec<u32>, QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.register);
        let version = self.upgrade_version(id);

        let mut registered = false;
        let mut staging = Vec::new();

        for cluster_id in target_cluster_ids {
            let Some(centroid) = self.centroid(*cluster_id) else {
                continue;
            };

            let code = Code::<Vec<u8>>::quantize(&embedding, &centroid)
                .as_ref()
                .into();

            let Some(len) = self.append(*cluster_id, id, version, code) else {
                continue;
            };

            registered = true;

            let split_threshold = self
                .config
                .split_threshold
                .unwrap_or(default_split_threshold()) as usize;
            if len > split_threshold {
                staging.push(*cluster_id);
            }
        }

        if !registered {
            let code = Code::<Vec<u8>>::quantize(&embedding, &embedding)
                .as_ref()
                .into();
            let delta = QuantizedDelta {
                center: embedding,
                codes: vec![code],
                ids: vec![id],
                length: 1,
                versions: vec![version],
            };
            self.spawn(delta)?;
        }

        Ok(staging)
    }

    /// Apply epsilon and RNG filtering to navigate results.
    /// Returns up to `replica_count` cluster heads that pass both filters.
    fn rng_select(&self, candidates: &SearchResult) -> SearchResult {
        let first_distance = candidates.distances.first().copied().unwrap_or(0.0);
        let mut result = SearchResult::default();
        let nreplica_count = self
            .config
            .nreplica_count
            .unwrap_or(default_nreplica_count()) as usize;
        let write_rng_epsilon = self
            .config
            .write_rng_epsilon
            .unwrap_or(default_write_rng_epsilon());
        let write_rng_factor = self
            .config
            .write_rng_factor
            .unwrap_or(default_write_rng_factor());
        let mut selected_centroids = Vec::<Arc<_>>::with_capacity(nreplica_count);

        for (cluster_id, distance) in candidates.keys.iter().zip(candidates.distances.iter()) {
            // Epsilon filter
            if (distance - first_distance).abs() > write_rng_epsilon * first_distance.abs() {
                break;
            }

            let Some(center) = self.centroid(*cluster_id) else {
                continue;
            };

            // RNG filter
            if selected_centroids
                .iter()
                .any(|sel| write_rng_factor * self.distance(&center, sel).abs() <= distance.abs())
            {
                continue;
            }

            result.keys.push(*cluster_id);
            result.distances.push(*distance);
            selected_centroids.push(center);

            if result.keys.len() >= nreplica_count {
                break;
            }
        }

        result
    }

    /// Normalize (if cosine) and rotate a vector for RaBitQ quantization.
    fn rotate(&self, embedding: &[f32]) -> Arc<[f32]> {
        let rotated = match self.distance_function {
            DistanceFunction::Cosine => {
                let normalized = normalize(embedding);
                &self.rotation * ColRef::from_slice(&normalized)
            }
            _ => &self.rotation * ColRef::from_slice(embedding),
        };
        rotated.iter().copied().collect()
    }

    /// Scrub a cluster: load from reader, remove invalid entries, update length.
    /// Does NOT trigger split/merge - use balance() for that.
    /// Returns the new length after scrubbing, or None if cluster not found.
    async fn scrub(&self, cluster_id: u32) -> Result<Option<usize>, QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.scrub);
        self.load(cluster_id).await?;

        let new_len = if let Some(mut delta) = self.cluster_deltas.get_mut(&cluster_id) {
            // Scrub: keep only valid entries
            let mut i = 0;
            while i < delta.ids.len() {
                if self.is_valid(delta.ids[i], delta.versions[i]) {
                    i += 1;
                } else {
                    delta.codes.swap_remove(i);
                    delta.ids.swap_remove(i);
                    delta.versions.swap_remove(i);
                }
            }
            delta.length = delta.ids.len();
            Some(delta.length)
        } else {
            None
        };

        Ok(new_len)
    }

    /// Spawn a new cluster and register it in the centroid index.
    fn spawn(&self, delta: QuantizedDelta) -> Result<u32, QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.spawn);
        let cluster_id = self.next_cluster_id.fetch_add(1, Ordering::Relaxed);
        let center = delta.center.clone();
        self.cluster_deltas.insert(cluster_id, delta);
        self.raw_centroid
            .add(cluster_id, &center)
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
        self.quantized_centroid
            .add(cluster_id, &center)
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
        Ok(cluster_id)
    }

    /// Split a large cluster into two smaller clusters using 2-means clustering.
    async fn split(&self, cluster_id: u32, depth: u32) -> Result<(), QuantizedSpannError> {
        let _guard = stats::TimedGuard::new(&self.stats.split);
        let Some(delta) = self.drop(cluster_id).await? else {
            return Ok(());
        };
        let old_center = delta.center.clone();

        let embeddings = delta
            .ids
            .iter()
            .zip(delta.versions.iter())
            .filter_map(|(id, version)| {
                self.is_valid(*id, *version)
                    .then(|| {
                        self.embeddings
                            .get(id)
                            .map(|emb| (*id, *version, emb.clone()))
                    })
                    .flatten()
            })
            .collect::<Vec<_>>();

        let split_threshold = self
            .config
            .split_threshold
            .unwrap_or(default_split_threshold()) as usize;
        if embeddings.len() <= split_threshold {
            self.spawn(delta)?;
            return Ok(());
        }

        let (left_center, left_group, right_center, right_group) =
            utils::split(embeddings, &self.distance_function);

        let left_delta = QuantizedDelta {
            center: left_center.clone(),
            codes: left_group
                .iter()
                .map(|(_, _, emb)| Code::<Vec<u8>>::quantize(emb, &left_center).as_ref().into())
                .collect(),
            ids: left_group.iter().map(|(id, _, _)| *id).collect(),
            length: left_group.len(),
            versions: left_group.iter().map(|(_, version, _)| *version).collect(),
        };
        let left_cluster_id = self.spawn(left_delta)?;

        let right_delta = QuantizedDelta {
            center: right_center.clone(),
            codes: right_group
                .iter()
                .map(|(_, _, emb)| {
                    Code::<Vec<u8>>::quantize(emb, &right_center)
                        .as_ref()
                        .into()
                })
                .collect(),
            ids: right_group.iter().map(|(id, _, _)| *id).collect(),
            length: right_group.len(),
            versions: right_group.iter().map(|(_, version, _)| *version).collect(),
        };
        let right_cluster_id = self.spawn(right_delta)?;

        if depth > MAX_BALANCE_DEPTH {
            return Ok(());
        }

        // NPA check for split points
        let evaluated = DashSet::new();
        for (id, version, embedding) in &left_group {
            if !self.is_valid(*id, *version) {
                continue;
            }
            if !evaluated.insert(*id) {
                continue;
            }
            let old_dist = self.distance(embedding, &old_center);
            let new_dist = self.distance(embedding, &left_center);
            if new_dist > old_dist {
                self.reassign(left_cluster_id, *id, *version, embedding.clone(), depth)
                    .await?;
            }
        }
        for (id, version, embedding) in &right_group {
            if !self.is_valid(*id, *version) {
                continue;
            }
            if !evaluated.insert(*id) {
                continue;
            }
            let old_dist = self.distance(embedding, &old_center);
            let new_dist = self.distance(embedding, &right_center);
            if new_dist > old_dist {
                self.reassign(right_cluster_id, *id, *version, embedding.clone(), depth)
                    .await?;
            }
        }

        // NPA check for neighbor points
        let mut reassign_candidates = Vec::new();
        let old_q_norm = f32::dot(&old_center, &old_center).unwrap_or(0.0).sqrt() as f32;
        let left_q_norm = if left_cluster_id == cluster_id {
            old_q_norm
        } else {
            f32::dot(&left_center, &left_center).unwrap_or(0.0).sqrt() as f32
        };
        let right_q_norm = if right_cluster_id == cluster_id {
            old_q_norm
        } else {
            f32::dot(&right_center, &right_center).unwrap_or(0.0).sqrt() as f32
        };

        let reassign_neighbor_count =
            self.config
                .reassign_neighbor_count
                .unwrap_or(default_reassign_neighbor_count()) as usize;
        let neighbors = self.navigate(&old_center, reassign_neighbor_count)?;
        for neighbor_id in neighbors.keys {
            if neighbor_id == cluster_id
                || neighbor_id == left_cluster_id
                || neighbor_id == right_cluster_id
            {
                continue;
            }
            self.scrub(neighbor_id).await?;
            let Some(neighbor_delta) = self.cluster_deltas.get(&neighbor_id).map(|d| d.clone())
            else {
                continue;
            };

            let c_norm = f32::dot(&neighbor_delta.center, &neighbor_delta.center)
                .unwrap_or(0.0)
                .sqrt() as f32;

            let old_r_q = old_center
                .iter()
                .zip(neighbor_delta.center.iter())
                .map(|(a, b)| a - b)
                .collect::<Vec<_>>();
            let old_c_dot_q = f32::dot(&neighbor_delta.center, &old_center).unwrap_or(0.0) as f32;

            let (left_r_q, left_c_dot_q) = if left_cluster_id == cluster_id {
                (old_r_q.clone(), old_c_dot_q)
            } else {
                let r_q = left_center
                    .iter()
                    .zip(neighbor_delta.center.iter())
                    .map(|(a, b)| a - b)
                    .collect::<Vec<_>>();
                let c_dot_q = f32::dot(&neighbor_delta.center, &left_center).unwrap_or(0.0) as f32;
                (r_q, c_dot_q)
            };

            let (right_r_q, right_c_dot_q) = if right_cluster_id == cluster_id {
                (old_r_q.clone(), old_c_dot_q)
            } else {
                let r_q = right_center
                    .iter()
                    .zip(neighbor_delta.center.iter())
                    .map(|(a, b)| a - b)
                    .collect::<Vec<_>>();
                let c_dot_q = f32::dot(&neighbor_delta.center, &right_center).unwrap_or(0.0) as f32;
                (r_q, c_dot_q)
            };

            let neighbor_r_q = vec![0.0; neighbor_delta.center.len()];
            let neighbor_c_dot_q = c_norm * c_norm;
            let neighbor_q_norm = c_norm;

            for (i, code) in neighbor_delta.codes.iter().enumerate() {
                let id = neighbor_delta.ids[i];
                let version = neighbor_delta.versions[i];

                if !self.is_valid(id, version) {
                    continue;
                }
                if !evaluated.insert(id) {
                    continue;
                }

                let code = Code::<&[u8]>::new(code.as_ref());

                let left_dist = code.distance_query(
                    &self.distance_function,
                    &left_r_q,
                    c_norm,
                    left_c_dot_q,
                    left_q_norm,
                );
                let right_dist = code.distance_query(
                    &self.distance_function,
                    &right_r_q,
                    c_norm,
                    right_c_dot_q,
                    right_q_norm,
                );
                let neighbor_dist = code.distance_query(
                    &self.distance_function,
                    &neighbor_r_q,
                    c_norm,
                    neighbor_c_dot_q,
                    neighbor_q_norm,
                );

                if neighbor_dist <= left_dist && neighbor_dist <= right_dist {
                    continue;
                }

                let old_dist = code.distance_query(
                    &self.distance_function,
                    &old_r_q,
                    c_norm,
                    old_c_dot_q,
                    old_q_norm,
                );

                if old_dist <= left_dist && old_dist <= right_dist {
                    continue;
                }

                reassign_candidates.push((neighbor_id, id, version));
            }
        }

        let candidate_ids = reassign_candidates
            .iter()
            .map(|(_, id, _)| *id)
            .collect::<Vec<_>>();
        self.load_raw(&candidate_ids).await?;

        for (from_cluster_id, id, version) in reassign_candidates {
            let Some(embedding) = self.embeddings.get(&id).map(|e| e.clone()) else {
                continue;
            };
            self.reassign(from_cluster_id, id, version, embedding, depth)
                .await?;
        }

        Ok(())
    }

    /// Increment and return the next version for a vector.
    fn upgrade_version(&self, id: u32) -> u32 {
        let mut entry = self.versions.entry(id).or_default();
        *entry += 1;
        *entry
    }
}

impl QuantizedSpannIndexWriter<USearchIndex> {
    /// Commit all in-memory state to blockfile writers and return a flusher.
    ///
    /// This method consumes the index and prepares all data for persistence.
    /// Call `finish()` before this method, then `flush()` on the returned
    /// flusher to actually write to storage.
    pub async fn commit(
        self,
        blockfile_provider: &BlockfileProvider,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<QuantizedSpannFlusher, QuantizedSpannError> {
        // === Create blockfile writers ===
        let mut qc_options = BlockfileWriterOptions::new(self.prefix_path.clone())
            .ordered_mutations()
            .max_block_size_bytes(self.cluster_block_size);
        let mut sm_options =
            BlockfileWriterOptions::new(self.prefix_path.clone()).ordered_mutations();
        let mut em_options =
            BlockfileWriterOptions::new(self.prefix_path.clone()).ordered_mutations();

        if let Some(file_ids) = &self.file_ids {
            qc_options = qc_options.fork(file_ids.quantized_cluster_id);
            em_options = em_options.fork(file_ids.embedding_metadata_id);
        }

        if let Some(cmek) = &self.cmek {
            qc_options = qc_options.with_cmek(cmek.clone());
            sm_options = sm_options.with_cmek(cmek.clone());
            em_options = em_options.with_cmek(cmek.clone());
        }

        let quantized_cluster_writer = blockfile_provider
            .write::<u32, QuantizedCluster<'_>>(qc_options)
            .await
            .map_err(|err| QuantizedSpannError::Blockfile(err.boxed()))?;

        let scalar_metadata_writer = blockfile_provider
            .write::<u32, u32>(sm_options)
            .await
            .map_err(|err| QuantizedSpannError::Blockfile(err.boxed()))?;

        let embedding_metadata_writer = blockfile_provider
            .write::<u32, Vec<f32>>(em_options)
            .await
            .map_err(|err| QuantizedSpannError::Blockfile(err.boxed()))?;

        // === Write quantized_cluster data ===
        let quantized_cluster_flusher = async {
            // Collect clusters that received mutations plus tombstones.
            let mut mutated_cluster_ids = self
                .cluster_deltas
                .iter()
                .filter_map(|entry| (!entry.value().ids.is_empty()).then_some(*entry.key()))
                .collect::<Vec<_>>();
            for cluster_id in self.tombstones.iter() {
                mutated_cluster_ids.push(*cluster_id);
            }

            // Sort for ordered mutations
            mutated_cluster_ids.sort_unstable();

            // Apply changes in order
            for cluster_id in mutated_cluster_ids {
                if let Some(delta) = self.cluster_deltas.get(&cluster_id) {
                    let codes = delta
                        .codes
                        .iter()
                        .flat_map(|c| c.iter())
                        .copied()
                        .collect::<Vec<_>>();
                    let cluster_ref = QuantizedCluster {
                        center: &delta.center,
                        codes: &codes,
                        ids: &delta.ids,
                        versions: &delta.versions,
                    };
                    quantized_cluster_writer
                        .set("", cluster_id, cluster_ref)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                } else {
                    quantized_cluster_writer
                        .delete::<_, QuantizedCluster<'_>>("", cluster_id)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                }
            }

            quantized_cluster_writer
                .commit::<u32, QuantizedCluster<'_>>()
                .await
                .map_err(QuantizedSpannError::Blockfile)
        }
        .instrument(tracing::trace_span!("Commit quantized cluster blockfile"))
        .await?;

        // === Write scalar_metadata ===
        let scalar_metadata_flusher = async {
            // 1. PREFIX_LENGTH - sorted by cluster_id
            let mut lengths = self
                .cluster_deltas
                .iter()
                .map(|entry| (*entry.key(), entry.value().length as u32))
                .collect::<Vec<_>>();
            lengths.sort_unstable();
            for (cluster_id, length) in lengths {
                scalar_metadata_writer
                    .set(PREFIX_LENGTH, cluster_id, length)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?;
            }

            // 2. PREFIX_NEXT_CLUSTER - single entry
            let next_id = self.next_cluster_id.load(Ordering::Relaxed);
            scalar_metadata_writer
                .set(PREFIX_NEXT_CLUSTER, SINGLETON_KEY, next_id)
                .await
                .map_err(QuantizedSpannError::Blockfile)?;

            // 3. PREFIX_VERSION - sorted by point_id
            let mut versions = self
                .versions
                .iter()
                .map(|entry| (*entry.key(), *entry.value()))
                .collect::<Vec<_>>();
            versions.sort_unstable();
            for (point_id, version) in versions {
                scalar_metadata_writer
                    .set(PREFIX_VERSION, point_id, version)
                    .await
                    .map_err(QuantizedSpannError::Blockfile)?;
            }

            scalar_metadata_writer
                .commit::<u32, u32>()
                .await
                .map_err(QuantizedSpannError::Blockfile)
        }
        .instrument(tracing::trace_span!("Commit scalar metadata blockfile"))
        .await?;

        // === Write embedding_metadata ===
        let embedding_metadata_flusher = async {
            // 1. PREFIX_CENTER - quantization center (always write, may be updated)
            embedding_metadata_writer
                .set(PREFIX_CENTER, SINGLETON_KEY, self.center.to_vec())
                .await
                .map_err(QuantizedSpannError::Blockfile)?;

            // 2. PREFIX_ROTATION - rotation matrix columns (only write for new indexes)
            if self.file_ids.is_none() {
                let dim = self.center.len();
                for col_idx in 0..dim {
                    let column = (0..dim)
                        .map(|row| self.rotation[(row, col_idx)])
                        .collect::<Vec<_>>();
                    embedding_metadata_writer
                        .set(PREFIX_ROTATION, col_idx as u32, column)
                        .await
                        .map_err(QuantizedSpannError::Blockfile)?;
                }
            }

            embedding_metadata_writer
                .commit::<u32, Vec<f32>>()
                .await
                .map_err(QuantizedSpannError::Blockfile)
        }
        .instrument(tracing::trace_span!("Commit embedding metadata blockfile"))
        .await?;

        Ok(QuantizedSpannFlusher {
            embedding_metadata_flusher,
            prefix_path: self.prefix_path.clone(),
            quantized_centroid: self.quantized_centroid,
            quantized_cluster_flusher,
            raw_centroid: self.raw_centroid,
            scalar_metadata_flusher,
            usearch_provider: usearch_provider.clone(),
        })
    }

    /// Create a new quantized SPANN index.
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        cluster_block_size: usize,
        collection_id: CollectionUuid,
        config: SpannIndexConfig,
        dimension: usize,
        distance_function: DistanceFunction,
        cmek: Option<Cmek>,
        prefix_path: String,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<Self, QuantizedSpannError> {
        // Create random rotation matrix
        let dist = UnitaryMat {
            dim: dimension,
            standard_normal: StandardNormal,
        };
        let rotation = dist.sample(&mut ThreadRng::default());
        let center = Arc::<[f32]>::from(vec![0.0; dimension]);

        // Get config values with defaults
        let max_neighbors = config.max_neighbors.unwrap_or(default_m_spann());
        let ef_construction = config
            .ef_construction
            .unwrap_or(default_construction_ef_spann());
        let ef_search = config.ef_search.unwrap_or(default_search_ef_spann());

        // Build USearch config from params
        let usearch_config = USearchIndexConfig {
            collection_id,
            cmek: cmek.clone(),
            prefix_path: prefix_path.clone(),
            dimensions: dimension,
            distance_function: distance_function.clone(),
            connectivity: max_neighbors,
            expansion_add: ef_construction,
            expansion_search: ef_search,
            quantization_center: None,
        };

        // Create centroid indexes
        let raw_centroid = usearch_provider
            .open(&usearch_config, OpenMode::Create)
            .instrument(tracing::trace_span!("Create raw centroid index"))
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        let quantized_usearch_config = USearchIndexConfig {
            quantization_center: Some(center.clone()),
            ..usearch_config
        };
        let quantized_centroid = usearch_provider
            .open(&quantized_usearch_config, OpenMode::Create)
            .instrument(tracing::trace_span!("Create quantized centroid index"))
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        Ok(Self {
            // === Config ===
            cluster_block_size,
            cmek,
            collection_id,
            config,
            dimension,
            distance_function,
            file_ids: None,
            prefix_path,
            // === Centroid Index ===
            next_cluster_id: Arc::new(AtomicU32::new(0)),
            quantized_centroid,
            raw_centroid,
            // === Quantization ===
            center,
            rotation,
            // === In-Memory State ===
            cluster_deltas: DashMap::new().into(),
            embeddings: DashMap::new().into(),
            tombstones: DashSet::new().into(),
            versions: DashMap::new().into(),
            // === Blockfile Readers ===
            quantized_cluster_reader: None,
            raw_embedding_reader: None,
            // === Dedup Sets ===
            balancing: DashSet::new().into(),
            // === Statistics ===
            stats: Arc::new(QuantizedSpannStats::default()),
        })
    }

    /// Prepare the index for commit: scrub mutated clusters, drop empty
    /// clusters, and rebuild centroid indexes if the quantization center
    /// has drifted. Must be called before `commit()`.
    pub async fn finish(
        &mut self,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<(), QuantizedSpannError> {
        // Scrub all clusters that received mutations.
        let mut mutated_cluster_ids = self
            .cluster_deltas
            .iter()
            .filter_map(|entry| (!entry.value().ids.is_empty()).then_some(*entry.key()))
            .collect::<Vec<_>>();
        mutated_cluster_ids.sort_unstable();

        for cluster_id in &mutated_cluster_ids {
            self.scrub(*cluster_id).await?;
        }

        // Drop clusters that ended up empty after scrubbing.
        let zero_length_cluster_ids = self
            .cluster_deltas
            .iter()
            .filter_map(|entry| (entry.value().length == 0).then_some(*entry.key()))
            .collect::<Vec<_>>();

        for cluster_id in zero_length_cluster_ids {
            self.drop(cluster_id).await?;
        }

        // Check center drift and rebuild centroid indexes if needed.
        self.rebuild_on_drift(usearch_provider)
            .instrument(tracing::trace_span!("Check center drift and rebuild"))
            .await?;

        Ok(())
    }

    /// Open an existing quantized SPANN index from file IDs.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        cluster_block_size: usize,
        collection_id: CollectionUuid,
        config: SpannIndexConfig,
        dimension: usize,
        distance_function: DistanceFunction,
        file_ids: QuantizedSpannIds,
        cmek: Option<Cmek>,
        prefix_path: String,
        raw_embedding_reader: Option<BlockfileReader<'static, u32, DataRecord<'static>>>,
        blockfile_provider: &BlockfileProvider,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<Self, QuantizedSpannError> {
        // Step 0: Load embedding_metadata (rotation matrix + quantization center)
        let (rotation, center) = async {
            let options =
                BlockfileReaderOptions::new(file_ids.embedding_metadata_id, prefix_path.clone());
            let reader = blockfile_provider
                .read::<u32, &'static [f32]>(options)
                .await
                .map_err(|e| QuantizedSpannError::Blockfile(e.boxed()))?;

            // Load rotation matrix columns
            let columns = reader
                .get_range(PREFIX_ROTATION..=PREFIX_ROTATION, ..)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
                .collect::<Vec<_>>();

            // Validate number of columns
            if columns.len() != dimension {
                return Err(QuantizedSpannError::DimensionMismatch {
                    expected: dimension,
                    got: columns.len(),
                });
            }

            // Validate each column length
            for (_prefix, _key, col) in &columns {
                if col.len() != dimension {
                    return Err(QuantizedSpannError::DimensionMismatch {
                        expected: dimension,
                        got: col.len(),
                    });
                }
            }

            // Construct rotation matrix column by column
            let rotation = Mat::from_fn(dimension, dimension, |i, j| columns[j].2[i]);

            // Load quantization center
            let center = reader
                .get(PREFIX_CENTER, SINGLETON_KEY)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
                .map(Arc::<[f32]>::from)
                .unwrap_or_else(|| vec![0.0; dimension].into());

            Ok((rotation, center))
        }
        .instrument(tracing::trace_span!(
            "Load rotation matrix and quantization center",
            dimension
        ))
        .await?;

        // Get config values with defaults
        let max_neighbors = config.max_neighbors.unwrap_or(default_m_spann());
        let ef_construction = config
            .ef_construction
            .unwrap_or(default_construction_ef_spann());
        let ef_search = config.ef_search.unwrap_or(default_search_ef_spann());

        // Build USearch config from params
        let usearch_config = USearchIndexConfig {
            collection_id,
            cmek: cmek.clone(),
            prefix_path: prefix_path.clone(),
            dimensions: dimension,
            distance_function: distance_function.clone(),
            connectivity: max_neighbors,
            expansion_add: ef_construction,
            expansion_search: ef_search,
            quantization_center: None,
        };

        // Step 1: Open centroid indexes
        let raw_centroid = usearch_provider
            .open(&usearch_config, OpenMode::Fork(file_ids.raw_centroid_id))
            .instrument(tracing::trace_span!(
                "Fork raw centroid index",
                index_id = %file_ids.raw_centroid_id.0
            ))
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        let quantized_usearch_config = USearchIndexConfig {
            quantization_center: Some(center.clone()),
            ..usearch_config
        };
        let quantized_centroid = usearch_provider
            .open(
                &quantized_usearch_config,
                OpenMode::Fork(file_ids.quantized_centroid_id),
            )
            .instrument(tracing::trace_span!(
                "Fork quantized centroid index",
                index_id = %file_ids.quantized_centroid_id.0
            ))
            .await
            .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;

        // Step 2: Load scalar metadata (next_cluster_id, versions, cluster_lengths)
        let (cluster_lengths, next_cluster_id, versions) = async {
            let options =
                BlockfileReaderOptions::new(file_ids.scalar_metadata_id, prefix_path.clone());
            let reader = blockfile_provider
                .read::<u32, u32>(options)
                .await
                .map_err(|err| QuantizedSpannError::Blockfile(err.boxed()))?;

            // Load cluster lengths
            let cluster_lengths = DashMap::new();
            for (_prefix, key, value) in reader
                .get_range(PREFIX_LENGTH..=PREFIX_LENGTH, ..)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
            {
                cluster_lengths.insert(key, value as usize);
            }

            // Load next_cluster_id
            let next_cluster_id = reader
                .get(PREFIX_NEXT_CLUSTER, SINGLETON_KEY)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
                .unwrap_or(0);

            // Load versions
            let versions = DashMap::new();
            for (_prefix, key, value) in reader
                .get_range(PREFIX_VERSION..=PREFIX_VERSION, ..)
                .await
                .map_err(QuantizedSpannError::Blockfile)?
            {
                versions.insert(key, value);
            }

            Ok::<_, QuantizedSpannError>((cluster_lengths, next_cluster_id, versions))
        }
        .instrument(tracing::trace_span!("Load scalar metadata"))
        .await?;

        // Step 3: Open cluster reader + initialize deltas from cluster_lengths
        let (quantized_cluster_reader, deltas) = async {
            let options =
                BlockfileReaderOptions::new(file_ids.quantized_cluster_id, prefix_path.clone());
            let reader = Some(
                blockfile_provider
                    .read(options)
                    .await
                    .map_err(|err| QuantizedSpannError::Blockfile(err.boxed()))?,
            );

            // Initialize deltas from cluster_lengths by getting centers from raw_centroid
            let deltas = DashMap::new();
            for entry in cluster_lengths.iter() {
                let cluster_id = *entry.key();
                let length = *entry.value();

                // Get center embedding from raw_centroid index
                if let Some(center_embedding) = raw_centroid
                    .get(cluster_id)
                    .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?
                {
                    deltas.insert(
                        cluster_id,
                        QuantizedDelta {
                            center: center_embedding.into(),
                            codes: Vec::new(),
                            ids: Vec::new(),
                            length,
                            versions: Vec::new(),
                        },
                    );
                }
            }

            Ok::<_, QuantizedSpannError>((reader, deltas))
        }
        .instrument(tracing::trace_span!(
            "Initialize cluster deltas",
            num_clusters = cluster_lengths.len()
        ))
        .await?;

        Ok(Self {
            // === Config ===
            cluster_block_size,
            cmek,
            collection_id,
            config,
            dimension,
            distance_function,
            file_ids: Some(file_ids),
            prefix_path,
            // === Centroid Index ===
            next_cluster_id: Arc::new(AtomicU32::new(next_cluster_id)),
            quantized_centroid,
            raw_centroid,
            // === Quantization ===
            center,
            rotation,
            // === In-Memory State ===
            cluster_deltas: deltas.into(),
            embeddings: DashMap::new().into(),
            tombstones: DashSet::new().into(),
            versions: versions.into(),
            // === Blockfile Readers ===
            quantized_cluster_reader,
            raw_embedding_reader,
            // === Dedup Sets ===
            balancing: DashSet::new().into(),
            // === Statistics ===
            stats: Arc::new(QuantizedSpannStats::default()),
        })
    }

    /// Check if the quantization center has drifted and rebuild centroid indexes if needed.
    /// Mutates `self.center` if rebuild occurs.
    async fn rebuild_on_drift(
        &mut self,
        usearch_provider: &USearchIndexProvider,
    ) -> Result<(), QuantizedSpannError> {
        // Compute new center by averaging all cluster centroids
        let dim = self.center.len();
        let mut new_center = vec![0.0f32; dim];
        for delta in self.cluster_deltas.iter() {
            for (acc_dim, dim) in new_center.iter_mut().zip(delta.center.iter()) {
                *acc_dim += *dim;
            }
        }
        for acc_dim in new_center.iter_mut() {
            *acc_dim /= self.cluster_deltas.len().max(1) as f32;
        }

        // Calculate drift distance
        let diff = new_center
            .iter()
            .zip(self.center.iter())
            .map(|(a, b)| a - b)
            .collect::<Vec<_>>();
        let drift_dist_sq = f32::dot(&diff, &diff).unwrap_or(0.0) as f32;
        let center_norm_sq = f32::dot(&new_center, &new_center).unwrap_or(0.0) as f32;

        let center_drift_threshold = self
            .config
            .center_drift_threshold
            .unwrap_or(default_center_drift_threshold());

        // Check if drift exceeds threshold
        let rebuilding = drift_dist_sq > center_drift_threshold.powi(2) * center_norm_sq;
        tracing::info!(
            drift_dist_sq,
            center_norm_sq,
            center_drift_threshold,
            rebuilding,
            "Center drift check"
        );

        if rebuilding {
            let max_neighbors = self.config.max_neighbors.unwrap_or(default_m_spann());
            let ef_construction = self
                .config
                .ef_construction
                .unwrap_or(default_construction_ef_spann());
            let ef_search = self.config.ef_search.unwrap_or(default_search_ef_spann());

            // Build USearch config from stored fields
            let usearch_config = USearchIndexConfig {
                collection_id: self.collection_id,
                cmek: self.cmek.clone(),
                prefix_path: self.prefix_path.clone(),
                dimensions: self.dimension,
                distance_function: self.distance_function.clone(),
                connectivity: max_neighbors,
                expansion_add: ef_construction,
                expansion_search: ef_search,
                quantization_center: None,
            };

            // Rebuild raw centroid index
            self.raw_centroid = usearch_provider
                .open(&usearch_config, OpenMode::Create)
                .await
                .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;

            // Rebuild quantized centroid index with new center
            let quantized_config = USearchIndexConfig {
                quantization_center: Some(new_center.clone().into()),
                ..usearch_config
            };
            self.quantized_centroid = usearch_provider
                .open(&quantized_config, OpenMode::Create)
                .await
                .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;

            // Re-add all cluster centers to both indexes
            for entry in self.cluster_deltas.iter() {
                let cluster_id = *entry.key();
                self.raw_centroid
                    .add(cluster_id, &entry.center)
                    .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
                self.quantized_centroid
                    .add(cluster_id, &entry.center)
                    .map_err(|err| QuantizedSpannError::CentroidIndex(err.boxed()))?;
            }

            // Update center
            self.center = new_center.into();
        }

        Ok(())
    }
}

/// Flusher for persisting a quantized SPANN index to storage.
pub struct QuantizedSpannFlusher {
    embedding_metadata_flusher: BlockfileFlusher,
    prefix_path: String,
    quantized_centroid: USearchIndex,
    quantized_cluster_flusher: BlockfileFlusher,
    raw_centroid: USearchIndex,
    scalar_metadata_flusher: BlockfileFlusher,
    usearch_provider: USearchIndexProvider,
}

impl QuantizedSpannFlusher {
    /// Flush all data to storage and return the file IDs.
    pub async fn flush(self) -> Result<QuantizedSpannIds, QuantizedSpannError> {
        // Get IDs before flushing
        let embedding_metadata_id = self.embedding_metadata_flusher.id();
        let quantized_cluster_id = self.quantized_cluster_flusher.id();
        let scalar_metadata_id = self.scalar_metadata_flusher.id();

        // Flush blockfiles
        self.embedding_metadata_flusher
            .flush::<u32, Vec<f32>>()
            .instrument(tracing::trace_span!("Flush embedding metadata blockfile"))
            .await
            .map_err(QuantizedSpannError::Blockfile)?;
        self.quantized_cluster_flusher
            .flush::<u32, QuantizedCluster<'_>>()
            .instrument(tracing::trace_span!("Flush quantized cluster blockfile"))
            .await
            .map_err(QuantizedSpannError::Blockfile)?;
        self.scalar_metadata_flusher
            .flush::<u32, u32>()
            .instrument(tracing::trace_span!("Flush scalar metadata blockfile"))
            .await
            .map_err(QuantizedSpannError::Blockfile)?;

        // Flush centroid indexes
        let quantized_centroid_id = self
            .usearch_provider
            .flush(&self.quantized_centroid)
            .instrument(tracing::trace_span!("Flush quantized centroid index"))
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;
        let raw_centroid_id = self
            .usearch_provider
            .flush(&self.raw_centroid)
            .instrument(tracing::trace_span!("Flush raw centroid index"))
            .await
            .map_err(|e| QuantizedSpannError::CentroidIndex(e.boxed()))?;

        // Return file IDs
        Ok(QuantizedSpannIds {
            embedding_metadata_id,
            prefix_path: self.prefix_path.clone(),
            quantized_centroid_id,
            quantized_cluster_id,
            raw_centroid_id,
            scalar_metadata_id,
        })
    }
}
#[cfg(test)]
mod tests {
    use std::sync::{atomic::Ordering, Arc};

    use chroma_blockstore::{
        arrow::{
            config::TEST_MAX_BLOCK_SIZE_BYTES,
            provider::{ArrowBlockfileProvider, BlockfileReaderOptions},
        },
        provider::BlockfileProvider,
        BlockfileWriterOptions,
    };
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    use chroma_distance::DistanceFunction;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{CollectionUuid, DataRecord, Quantization, SpannIndexConfig};
    use rand::{Rng, SeedableRng};
    use tempfile::TempDir;

    use crate::{
        quantization::Code,
        usearch::{USearchIndex, USearchIndexProvider},
        VectorIndex,
    };

    use super::{QuantizedDelta, QuantizedSpannIndexWriter};

    const TEST_CLUSTER_BLOCK_SIZE: usize = 2 * 1024 * 1024;
    const TEST_DIMENSION: usize = 4;
    const TEST_EPSILON: f32 = 1e-5;

    fn test_config() -> SpannIndexConfig {
        SpannIndexConfig {
            write_nprobe: Some(4),
            nreplica_count: Some(2),
            write_rng_epsilon: Some(4.0),
            write_rng_factor: Some(1.0),
            split_threshold: Some(8),
            merge_threshold: Some(2),
            reassign_neighbor_count: Some(6),
            center_drift_threshold: Some(0.125),
            search_nprobe: Some(4),
            search_rng_epsilon: Some(4.0),
            search_rng_factor: Some(1.0),
            ef_construction: Some(32),
            ef_search: Some(16),
            num_samples_kmeans: None,
            initial_lambda: None,
            num_centers_to_merge_to: None,
            max_neighbors: Some(8),
            quantize: Quantization::FourBitRabitQWithUSearch,
        }
    }

    fn test_distance_function() -> DistanceFunction {
        DistanceFunction::Cosine
    }

    fn test_storage(tmp_dir: &TempDir) -> Storage {
        Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()))
    }

    fn test_blockfile_provider(storage: Storage) -> BlockfileProvider {
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            16,
        );
        BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider)
    }

    fn test_usearch_provider(storage: Storage) -> USearchIndexProvider {
        let usearch_cache = new_non_persistent_cache_for_test();
        USearchIndexProvider::new(storage, usearch_cache)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        // === Setup ===
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&tmp_dir);
        let usearch_provider = test_usearch_provider(storage);

        let writer = QuantizedSpannIndexWriter::<USearchIndex>::create(
            TEST_CLUSTER_BLOCK_SIZE,
            CollectionUuid::new(),
            test_config(),
            TEST_DIMENSION,
            test_distance_function(),
            None,
            "".to_string(),
            &usearch_provider,
        )
        .await
        .expect("Failed to create writer");

        // =======================================================================
        // Level 0: Pure/Accessor Operations
        // =======================================================================

        // --- upgrade ---
        let v1 = writer.upgrade_version(1);
        assert_eq!(v1, 1);
        assert_eq!(writer.versions.get(&1).map(|v| *v), Some(1));

        let v2 = writer.upgrade_version(1);
        assert_eq!(v2, 2);
        assert_eq!(writer.versions.get(&1).map(|v| *v), Some(2));

        let v3 = writer.upgrade_version(2);
        assert_eq!(v3, 1);
        assert_eq!(writer.versions.get(&2).map(|v| *v), Some(1));

        // --- is_valid ---
        assert!(writer.is_valid(1, 2)); // current version
        assert!(!writer.is_valid(1, 1)); // stale
        assert!(!writer.is_valid(1, 3)); // future
        assert!(!writer.is_valid(999, 1)); // unknown id

        // --- distance (Cosine) ---
        // Cosine distance = 1 - cos(theta)
        // Identical vectors: cos = 1, distance = 0
        assert!(writer.distance(&[1.0, 0.0, 0.0, 0.0], &[1.0, 0.0, 0.0, 0.0]) < TEST_EPSILON);
        // Opposite vectors: cos = -1, distance = 2
        assert!(
            (writer.distance(&[1.0, 0.0, 0.0, 0.0], &[-1.0, 0.0, 0.0, 0.0]) - 2.0).abs()
                < TEST_EPSILON
        );
        // Orthogonal vectors: cos = 0, distance = 1
        assert!(
            (writer.distance(&[1.0, 0.0, 0.0, 0.0], &[0.0, 1.0, 0.0, 0.0]) - 1.0).abs()
                < TEST_EPSILON
        );

        // --- rotate ---
        // For cosine space: normalize first, then rotate. Rotation preserves norm.
        let rotated = writer.rotate(&[1.0, 0.0, 0.0, 0.0]);
        assert_eq!(rotated.len(), TEST_DIMENSION);
        let norm = rotated.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < TEST_EPSILON,
            "Expected norm ~1.0, got {}",
            norm
        );

        // Non-unit vector should also result in norm ~1.0 after rotation (due to normalization)
        let rotated2 = writer.rotate(&[2.0, 0.0, 0.0, 0.0]);
        let norm2 = rotated2.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm2 - 1.0).abs() < TEST_EPSILON,
            "Expected norm ~1.0, got {}",
            norm2
        );

        // --- centroid (no clusters yet) ---
        assert!(writer.centroid(1).is_none());
        assert!(writer.centroid(999).is_none());

        // --- navigate (no clusters yet) ---
        let result = writer
            .navigate(&[1.0, 0.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(result.keys.is_empty());

        // --- rng_select (empty candidates) ---
        let empty_result = writer.rng_select(&result);
        assert!(empty_result.keys.is_empty());

        // =======================================================================
        // Level 1: Simple Mutations
        // =======================================================================

        // --- spawn ---
        let center1: Arc<[f32]> = Arc::from([1.0f32, 0.0, 0.0, 0.0]);
        let delta1 = QuantizedDelta {
            center: center1.clone(),
            codes: vec![],
            ids: vec![],
            length: 0,
            versions: vec![],
        };
        let next_id_before = writer.next_cluster_id.load(Ordering::Relaxed);
        let cluster_id_1 = writer.spawn(delta1).expect("spawn failed");
        assert_eq!(cluster_id_1, next_id_before);
        assert_eq!(
            writer.next_cluster_id.load(Ordering::Relaxed),
            next_id_before + 1
        );

        // Verify centroid is retrievable
        let retrieved_center = writer.centroid(cluster_id_1).expect("centroid not found");
        assert_eq!(retrieved_center.as_ref(), center1.as_ref());

        // --- navigate (with cluster) ---
        let result = writer
            .navigate(&[1.0, 0.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(!result.keys.is_empty());
        assert!(result.keys.contains(&cluster_id_1));

        // --- append ---
        // Create a test embedding for point 10 and quantize it relative to center1
        let emb_10 = [1.0f32, 0.1, 0.0, 0.0];
        let code_10: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_10, &center1).as_ref().into();
        let v10 = writer.upgrade_version(10);
        let new_len = writer.append(cluster_id_1, 10, v10, code_10.clone());
        assert_eq!(new_len, Some(1));

        // Verify delta has the point
        let delta = writer
            .cluster_deltas
            .get(&cluster_id_1)
            .expect("delta not found")
            .clone();
        assert!(delta.ids.contains(&10));
        assert_eq!(delta.length, 1);

        // Append to non-existent cluster returns None
        let v11 = writer.upgrade_version(11);
        let bad_append = writer.append(999, 11, v11, code_10.clone());
        assert!(bad_append.is_none());

        // --- spawn more clusters for RNG test ---
        let center2: Arc<[f32]> = Arc::from([0.0f32, 1.0, 0.0, 0.0]);
        let delta2 = QuantizedDelta {
            center: center2,
            codes: vec![],
            ids: vec![],
            length: 0,
            versions: vec![],
        };
        let cluster_id_2 = writer.spawn(delta2).expect("spawn failed");

        let center3: Arc<[f32]> = Arc::from([0.0f32, 0.0, 1.0, 0.0]);
        let delta3 = QuantizedDelta {
            center: center3,
            codes: vec![],
            ids: vec![],
            length: 0,
            versions: vec![],
        };
        let cluster_id_3 = writer.spawn(delta3).expect("spawn failed");

        // --- rng_select (with multiple clusters) ---
        // Query near cluster 1
        let candidates = writer
            .navigate(&[1.0, 0.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(!candidates.keys.is_empty());

        let selected = writer.rng_select(&candidates);
        // Should select at least the closest cluster
        assert!(!selected.keys.is_empty());
        // First selected should be cluster_id_1 (closest to query)
        assert_eq!(selected.keys[0], cluster_id_1);

        // --- drop ---
        writer.drop(cluster_id_2).await.expect("drop failed");

        // Verify tombstone
        assert!(writer.tombstones.contains(&cluster_id_2));

        // Navigate should NOT return dropped cluster
        let result = writer
            .navigate(&[0.0, 1.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(!result.keys.contains(&cluster_id_2));

        // Centroid returns None (drop removes from deltas too)
        assert!(writer.centroid(cluster_id_2).is_none());

        // Verify remaining clusters are still navigable
        let result = writer
            .navigate(&[1.0, 0.0, 0.0, 0.0], 5)
            .expect("navigate failed");
        assert!(result.keys.contains(&cluster_id_1));
        assert!(result.keys.contains(&cluster_id_3));
    }

    #[tokio::test]
    async fn test_load_and_scrub_operations() {
        // =======================================================================
        // Setup: Create raw embedding blockfile with test data
        // =======================================================================
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&tmp_dir);
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let usearch_provider = test_usearch_provider(storage.clone());
        let collection_id = CollectionUuid::new();

        // Raw embeddings for test points (distinct vectors for each id)
        let raw_embeddings = vec![
            (100u32, [1.0f32, 0.0, 0.0, 0.0]),
            (101, [0.0, 1.0, 0.0, 0.0]),
            (102, [0.0, 0.0, 1.0, 0.0]),
            (200, [0.0, 0.0, 0.0, 1.0]),
            (201, [0.5, 0.5, 0.0, 0.0]), // Will be invalidated
            (300, [0.5, 0.0, 0.5, 0.0]),
            (301, [0.0, 0.5, 0.5, 0.0]), // Will be invalidated
            (302, [0.0, 0.0, 0.5, 0.5]),
        ];

        // Create and populate raw embedding blockfile
        let raw_writer = blockfile_provider
            .write::<u32, &DataRecord<'_>>(
                BlockfileWriterOptions::new("".to_string()).ordered_mutations(),
            )
            .await
            .expect("Failed to create raw embedding writer");

        for (id, embedding) in &raw_embeddings {
            let record = DataRecord {
                id: "",
                embedding: embedding.as_slice(),
                metadata: None,
                document: None,
            };
            raw_writer
                .set("", *id, &record)
                .await
                .expect("Failed to write raw embedding");
        }

        let raw_flusher = raw_writer
            .commit::<u32, &DataRecord<'_>>()
            .await
            .expect("Failed to commit raw embeddings");
        let raw_embedding_id = raw_flusher.id();
        raw_flusher
            .flush::<u32, &DataRecord<'_>>()
            .await
            .expect("Failed to flush raw embeddings");

        // =======================================================================
        // Phase 1: Create index, add points, commit, flush
        // =======================================================================
        let mut writer = QuantizedSpannIndexWriter::<USearchIndex>::create(
            TEST_CLUSTER_BLOCK_SIZE,
            collection_id,
            test_config(),
            TEST_DIMENSION,
            test_distance_function(),
            None,
            "".to_string(),
            &usearch_provider,
        )
        .await
        .expect("Failed to create writer");

        // Spawn a cluster and add points 100, 101, 102
        let center: Arc<[f32]> = Arc::from([1.0f32, 0.0, 0.0, 0.0]);

        // Create properly quantized codes for each embedding relative to the center
        let emb_100 = [1.0f32, 0.0, 0.0, 0.0];
        let emb_101 = [0.0f32, 1.0, 0.0, 0.0];
        let emb_102 = [0.0f32, 0.0, 1.0, 0.0];
        let code_100: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_100, &center).as_ref().into();
        let code_101: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_101, &center).as_ref().into();
        let code_102: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_102, &center).as_ref().into();

        // Get versions via upgrade()
        let v100 = writer.upgrade_version(100);
        let v101 = writer.upgrade_version(101);
        let v102 = writer.upgrade_version(102);

        let delta = QuantizedDelta {
            center: center.clone(),
            codes: vec![code_100, code_101, code_102],
            ids: vec![100, 101, 102],
            length: 3,
            versions: vec![v100, v101, v102],
        };
        let cluster_id = writer.spawn(delta).expect("spawn failed");

        // Capture expected rotated embeddings for later verification
        // These are the raw embeddings from raw_embeddings array rotated by the current rotation matrix
        let expected_rotated_100 = writer.rotate(&[1.0, 0.0, 0.0, 0.0]);
        let expected_rotated_101 = writer.rotate(&[0.0, 1.0, 0.0, 0.0]);
        let expected_rotated_102 = writer.rotate(&[0.0, 0.0, 1.0, 0.0]);

        // Finish, commit, and flush
        writer
            .finish(&usearch_provider)
            .await
            .expect("Failed to finish");
        let flusher = Box::pin(writer.commit(&blockfile_provider, &usearch_provider))
            .await
            .expect("Failed to commit");
        let file_ids = Box::pin(flusher.flush()).await.expect("Failed to flush");

        // =======================================================================
        // Phase 2: Reopen index with readers and test load operations
        // =======================================================================
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let usearch_provider = test_usearch_provider(storage.clone());

        // Create raw embedding reader
        let raw_reader = blockfile_provider
            .read::<u32, DataRecord<'static>>(BlockfileReaderOptions::new(
                raw_embedding_id,
                "".to_string(),
            ))
            .await
            .expect("Failed to open raw embedding reader");

        let writer = QuantizedSpannIndexWriter::<USearchIndex>::open(
            TEST_CLUSTER_BLOCK_SIZE,
            collection_id,
            test_config(),
            TEST_DIMENSION,
            test_distance_function(),
            file_ids,
            None,
            "".to_string(),
            Some(raw_reader),
            &blockfile_provider,
            &usearch_provider,
        )
        .await
        .expect("Failed to open writer");

        // --- load ---
        // After open, delta exists but ids/codes/versions are empty (only length is set)
        {
            let delta = writer
                .cluster_deltas
                .get(&cluster_id)
                .expect("delta not found");
            assert_eq!(delta.length, 3);
            assert!(delta.ids.is_empty(), "ids should be empty before load");
        }

        // Call load to populate delta from blockfile
        writer.load(cluster_id).await.expect("load failed");

        {
            let delta = writer
                .cluster_deltas
                .get(&cluster_id)
                .expect("delta not found");
            assert_eq!(delta.ids.len(), 3);
            assert!(delta.ids.contains(&100));
            assert!(delta.ids.contains(&101));
            assert!(delta.ids.contains(&102));
        }

        // --- load_raw ---
        // Verify embeddings cache is empty
        assert!(writer.embeddings.get(&100).is_none());
        assert!(writer.embeddings.get(&101).is_none());
        assert!(writer.embeddings.get(&102).is_none());

        // Load raw embeddings
        writer
            .load_raw(&[100, 101, 102])
            .await
            .expect("load_raw failed");

        // Verify embeddings are now in cache and rotated consistently
        // The rotation matrix was persisted and reloaded, so rotate() should produce same results
        let loaded_100 = writer
            .embeddings
            .get(&100)
            .expect("embedding 100 not found")
            .clone();
        let loaded_101 = writer
            .embeddings
            .get(&101)
            .expect("embedding 101 not found")
            .clone();
        let loaded_102 = writer
            .embeddings
            .get(&102)
            .expect("embedding 102 not found")
            .clone();

        assert!(
            writer.distance(&loaded_100, &expected_rotated_100) < TEST_EPSILON,
            "rotation mismatch for id 100"
        );
        assert!(
            writer.distance(&loaded_101, &expected_rotated_101) < TEST_EPSILON,
            "rotation mismatch for id 101"
        );
        assert!(
            writer.distance(&loaded_102, &expected_rotated_102) < TEST_EPSILON,
            "rotation mismatch for id 102"
        );

        // --- drop ---
        // Spawn a new cluster with points 200, 201
        let center2: Arc<[f32]> = Arc::from([0.0f32, 0.0, 0.0, 1.0]);

        // Create properly quantized codes
        let emb_200 = [0.0f32, 0.0, 0.0, 1.0];
        let emb_201 = [0.0f32, 0.0, 0.5, 0.5];
        let code_200: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_200, &center2)
            .as_ref()
            .into();
        let code_201: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_201, &center2)
            .as_ref()
            .into();

        // Get versions via upgrade()
        let v200 = writer.upgrade_version(200);
        let v201 = writer.upgrade_version(201);

        let delta2 = QuantizedDelta {
            center: center2,
            codes: vec![code_200, code_201],
            ids: vec![200, 201],
            length: 2,
            versions: vec![v200, v201],
        };
        let cluster_id_2 = writer.spawn(delta2).expect("spawn failed");

        // Invalidate 201 by upgrading its version
        writer.upgrade_version(201); // Now version is 2, but cluster has version 1

        // Verify embedding 200 not in cache before drop
        assert!(writer.embeddings.get(&200).is_none());

        // Drop cluster - should load raw embeddings for valid point (200) only
        let dropped = writer
            .drop(cluster_id_2)
            .await
            .expect("drop failed")
            .expect("expected delta");
        assert_eq!(dropped.ids, vec![200, 201]);

        // Cluster should be removed from deltas
        assert!(writer.cluster_deltas.get(&cluster_id_2).is_none());

        // Embedding for valid point 200 should be loaded
        assert!(writer.embeddings.get(&200).is_some());

        // Cluster should be in tombstones
        assert!(writer.tombstones.contains(&cluster_id_2));

        // --- scrub ---
        // Spawn a cluster with points 300, 301, 302
        let center3: Arc<[f32]> = Arc::from([0.5f32, 0.0, 0.5, 0.0]);

        // Create properly quantized codes
        let emb_300 = [0.5f32, 0.0, 0.5, 0.0];
        let emb_301 = [0.5f32, 0.5, 0.0, 0.0];
        let emb_302 = [0.0f32, 0.5, 0.5, 0.0];
        let code_300: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_300, &center3)
            .as_ref()
            .into();
        let code_301: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_301, &center3)
            .as_ref()
            .into();
        let code_302: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_302, &center3)
            .as_ref()
            .into();

        // Get versions via upgrade()
        let v300 = writer.upgrade_version(300);
        let v301 = writer.upgrade_version(301);
        let v302 = writer.upgrade_version(302);

        let delta3 = QuantizedDelta {
            center: center3,
            codes: vec![code_300, code_301, code_302],
            ids: vec![300, 301, 302],
            length: 3,
            versions: vec![v300, v301, v302],
        };
        let cluster_id_3 = writer.spawn(delta3).expect("spawn failed");

        // Invalidate 301 by upgrading its version
        writer.upgrade_version(301); // Now version is 2

        // Before scrub: all 3 points in delta
        {
            let delta = writer
                .cluster_deltas
                .get(&cluster_id_3)
                .expect("delta not found");
            assert_eq!(delta.ids.len(), 3);
            assert_eq!(delta.length, 3);
        }

        // Scrub should remove invalid point 301
        let new_len = writer
            .scrub(cluster_id_3)
            .await
            .expect("scrub failed")
            .expect("expected length");
        assert_eq!(new_len, 2);

        // After scrub: only points 300, 302 remain
        {
            let delta = writer
                .cluster_deltas
                .get(&cluster_id_3)
                .expect("delta not found");
            assert_eq!(delta.ids.len(), 2);
            assert_eq!(delta.length, 2);
            assert!(delta.ids.contains(&300));
            assert!(!delta.ids.contains(&301)); // Removed
            assert!(delta.ids.contains(&302));
        }
    }

    #[tokio::test]
    async fn test_insert_and_balance_operations() {
        // =======================================================================
        // Setup
        // =======================================================================
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&tmp_dir);
        let usearch_provider = test_usearch_provider(storage);

        let writer = QuantizedSpannIndexWriter::<USearchIndex>::create(
            TEST_CLUSTER_BLOCK_SIZE,
            CollectionUuid::new(),
            test_config(),
            TEST_DIMENSION,
            test_distance_function(),
            None,
            "".to_string(),
            &usearch_provider,
        )
        .await
        .expect("Failed to create writer");

        // =======================================================================
        // Step 1: insert (empty index -> spawn)
        // =======================================================================
        // First insert on empty index should spawn a new cluster
        assert_eq!(writer.cluster_deltas.len(), 0);

        writer
            .add(1, &[1.0, 0.0, 0.0, 0.0])
            .await
            .expect("add failed");

        assert_eq!(writer.cluster_deltas.len(), 1);
        let first_cluster_id = *writer.cluster_deltas.iter().next().unwrap().key();

        // =======================================================================
        // Step 2: insert (append to existing cluster)
        // =======================================================================
        // Insert more points near the same center - should append, not spawn
        for id in 2..=5 {
            writer
                .add(id, &[1.0, 0.0, 0.0, 0.0])
                .await
                .expect("add failed");
        }

        // Still only 1 cluster
        assert_eq!(writer.cluster_deltas.len(), 1);

        // Cluster should have 5 points
        {
            let delta = writer
                .cluster_deltas
                .get(&first_cluster_id)
                .expect("delta not found");
            assert_eq!(delta.length, 5);
        }

        // =======================================================================
        // Step 3: split with reassign (triggered by balance)
        // =======================================================================
        // Geometry:
        // - neighbor_center: [0, 1, 0, 0]
        // - mixed_center: [0, 0.9, 0.1, 0] (close to neighbor, so navigate finds it)
        // - After split, new centers will be near [1, 0, 0, 0] and [-1, 0, 0, 0]
        // - Misplaced points [0.9, 0.1, 0, 0] and [-0.9, 0.1, 0, 0] in neighbor
        //   are closer to the new split centers than to neighbor_center

        // Neighbor cluster with incorrectly assigned points
        let neighbor_center: Arc<[f32]> = writer.rotate(&[0.0, 1.0, 0.0, 0.0]);

        // Points that are closer to [1, 0, 0, 0] or [-1, 0, 0, 0] than to [0, 1, 0, 0]
        let misplaced_emb_1 = writer.rotate(&[0.9, 0.1, 0.0, 0.0]);
        let misplaced_emb_2 = writer.rotate(&[-0.9, 0.1, 0.0, 0.0]);

        let v100 = writer.upgrade_version(100);
        let v101 = writer.upgrade_version(101);

        // Populate embeddings cache
        writer.embeddings.insert(100, misplaced_emb_1.clone());
        writer.embeddings.insert(101, misplaced_emb_2.clone());

        // Create proper quantization codes
        let code_100: Arc<[u8]> = Code::<Vec<u8>>::quantize(&misplaced_emb_1, &neighbor_center)
            .as_ref()
            .into();
        let code_101: Arc<[u8]> = Code::<Vec<u8>>::quantize(&misplaced_emb_2, &neighbor_center)
            .as_ref()
            .into();

        let neighbor_delta = QuantizedDelta {
            center: neighbor_center.clone(),
            codes: vec![code_100, code_101],
            ids: vec![100, 101],
            length: 2,
            versions: vec![v100, v101],
        };
        writer.spawn(neighbor_delta).expect("spawn failed");

        // Now we have 2 clusters
        assert_eq!(writer.cluster_deltas.len(), 2);

        // mixed_center close to neighbor so navigate from mixed_center finds neighbor
        // After split, points will move to centers near [1,0,0,0] and [-1,0,0,0]
        let mixed_center: Arc<[f32]> = writer.rotate(&[0.0, 0.9, 0.1, 0.0]);

        let mut mixed_ids = vec![];
        let mut mixed_versions = vec![];
        let mut mixed_codes = vec![];

        // Group A: 5 points near [1, 0, 0, 0] - will form one split cluster
        for id in 50..55 {
            let v = writer.upgrade_version(id);
            let emb = writer.rotate(&[1.0, 0.0, 0.0, 0.0]);
            let code: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb, &mixed_center)
                .as_ref()
                .into();
            writer.embeddings.insert(id, emb);
            mixed_ids.push(id);
            mixed_versions.push(v);
            mixed_codes.push(code);
        }

        // Group B: 5 points near [-1, 0, 0, 0] - will form another split cluster
        for id in 55..60 {
            let v = writer.upgrade_version(id);
            let emb = writer.rotate(&[-1.0, 0.0, 0.0, 0.0]);
            let code: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb, &mixed_center)
                .as_ref()
                .into();
            writer.embeddings.insert(id, emb);
            mixed_ids.push(id);
            mixed_versions.push(v);
            mixed_codes.push(code);
        }

        let mixed_delta = QuantizedDelta {
            center: mixed_center,
            codes: mixed_codes,
            ids: mixed_ids,
            length: 10,
            versions: mixed_versions,
        };
        let mixed_cluster_id = writer.spawn(mixed_delta).expect("spawn failed");

        // Now we have 3 clusters: first_cluster, neighbor, mixed
        assert_eq!(writer.cluster_deltas.len(), 3);

        // Trigger balance on the mixed cluster - should split into 2
        writer
            .balance(mixed_cluster_id, 0)
            .await
            .expect("balance failed");

        // After split, we should have more clusters
        // mixed_cluster splits into 2, so we have: first_cluster + neighbor + 2 from split = 4
        // (mixed_cluster itself may be dropped and replaced by 2 new ones)
        assert!(
            writer.cluster_deltas.len() >= 3,
            "Expected at least 3 clusters after split, got {}",
            writer.cluster_deltas.len()
        );

        // =======================================================================
        // Step 4: merge (triggered by balance after scrub)
        // =======================================================================
        // Spawn a small cluster far from others: [0, 0, 0, 1]
        let isolated_center: Arc<[f32]> = writer.rotate(&[0.0, 0.0, 0.0, 1.0]);

        // Add 4 points, will invalidate 3 to trigger merge
        let v200 = writer.upgrade_version(200);
        let v201 = writer.upgrade_version(201);
        let v202 = writer.upgrade_version(202);
        let v203 = writer.upgrade_version(203);

        // Create embeddings and codes for isolated cluster points
        let emb_200 = writer.rotate(&[0.0, 0.0, 0.0, 1.0]);
        let emb_201 = writer.rotate(&[0.0, 0.0, 0.1, 0.9]);
        let emb_202 = writer.rotate(&[0.0, 0.1, 0.0, 0.9]);
        let emb_203 = writer.rotate(&[0.1, 0.0, 0.0, 0.9]);

        writer.embeddings.insert(200, emb_200.clone());
        writer.embeddings.insert(201, emb_201.clone());
        writer.embeddings.insert(202, emb_202.clone());
        writer.embeddings.insert(203, emb_203.clone());

        let code_200: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_200, &isolated_center)
            .as_ref()
            .into();
        let code_201: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_201, &isolated_center)
            .as_ref()
            .into();
        let code_202: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_202, &isolated_center)
            .as_ref()
            .into();
        let code_203: Arc<[u8]> = Code::<Vec<u8>>::quantize(&emb_203, &isolated_center)
            .as_ref()
            .into();

        let isolated_delta = QuantizedDelta {
            center: isolated_center,
            codes: vec![code_200, code_201, code_202, code_203],
            ids: vec![200, 201, 202, 203],
            length: 4,
            versions: vec![v200, v201, v202, v203],
        };
        let isolated_cluster_id = writer.spawn(isolated_delta).expect("spawn failed");

        // Invalidate 3 points, leaving only 1 valid (below merge_threshold of 2)
        writer.upgrade_version(201);
        writer.upgrade_version(202);
        writer.upgrade_version(203);

        // Trigger balance - should scrub (remove invalid) then merge (below threshold)
        writer
            .balance(isolated_cluster_id, 0)
            .await
            .expect("balance failed");

        // Isolated cluster should be dropped (merged into neighbor)
        assert!(
            writer.cluster_deltas.get(&isolated_cluster_id).is_none(),
            "Isolated cluster should have been merged"
        );

        // Point 200 should now be in some other cluster (reassigned during merge)
        // Check that it exists somewhere with current version
        let current_v200 = *writer.versions.get(&200).expect("version not found");
        let point_200_found = writer.cluster_deltas.iter().any(|entry| {
            entry
                .value()
                .ids
                .iter()
                .zip(entry.value().versions.iter())
                .any(|(id, ver)| *id == 200 && *ver == current_v200)
        });
        assert!(
            point_200_found,
            "Point 200 should exist in some cluster after merge"
        );
    }

    #[tokio::test]
    async fn test_open_finish_commit() {
        // =======================================================================
        // Setup
        // =======================================================================
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&tmp_dir);
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let usearch_provider = test_usearch_provider(storage.clone());
        let collection_id = CollectionUuid::new();

        let mut writer = QuantizedSpannIndexWriter::<USearchIndex>::create(
            TEST_CLUSTER_BLOCK_SIZE,
            collection_id,
            test_config(),
            TEST_DIMENSION,
            test_distance_function(),
            None,
            "".to_string(),
            &usearch_provider,
        )
        .await
        .expect("Failed to create writer");

        let test_vector = [1.0f32, 2.0, 3.0, 4.0];
        let expected_rotated = writer.rotate(&test_vector);

        // --- Cluster A: normal cluster ---
        let center_a: Arc<[f32]> = Arc::from([1.0f32, 0.0, 0.0, 0.0]);
        let v10 = writer.upgrade_version(10);
        let v11 = writer.upgrade_version(11);
        let v12 = writer.upgrade_version(12);
        let code_a: Arc<[u8]> = Code::<Vec<u8>>::quantize(&[1.0, 0.0, 0.0, 0.0], &center_a)
            .as_ref()
            .into();
        let delta_a = QuantizedDelta {
            center: center_a.clone(),
            codes: vec![code_a.clone(), code_a.clone(), code_a.clone()],
            ids: vec![10, 11, 12],
            length: 3,
            versions: vec![v10, v11, v12],
        };
        let cluster_a = writer.spawn(delta_a).expect("spawn A failed");

        // --- Cluster B: partial invalidation ---
        let center_b: Arc<[f32]> = Arc::from([0.0f32, 1.0, 0.0, 0.0]);
        let v20 = writer.upgrade_version(20);
        let v21 = writer.upgrade_version(21);
        let v22 = writer.upgrade_version(22);
        let v23 = writer.upgrade_version(23);
        let code_b: Arc<[u8]> = Code::<Vec<u8>>::quantize(&[0.0, 1.0, 0.0, 0.0], &center_b)
            .as_ref()
            .into();
        let delta_b = QuantizedDelta {
            center: center_b.clone(),
            codes: vec![
                code_b.clone(),
                code_b.clone(),
                code_b.clone(),
                code_b.clone(),
            ],
            ids: vec![20, 21, 22, 23],
            length: 4,
            versions: vec![v20, v21, v22, v23],
        };
        let cluster_b = writer.spawn(delta_b).expect("spawn B failed");

        // --- Cluster C: full invalidation ---
        let center_c: Arc<[f32]> = Arc::from([0.0f32, 0.0, 1.0, 0.0]);
        let v30 = writer.upgrade_version(30);
        let v31 = writer.upgrade_version(31);
        let code_c: Arc<[u8]> = Code::<Vec<u8>>::quantize(&[0.0, 0.0, 1.0, 0.0], &center_c)
            .as_ref()
            .into();
        let delta_c = QuantizedDelta {
            center: center_c.clone(),
            codes: vec![code_c.clone(), code_c.clone()],
            ids: vec![30, 31],
            length: 2,
            versions: vec![v30, v31],
        };
        let cluster_c = writer.spawn(delta_c).expect("spawn C failed");

        // --- Cluster D: tombstoned ---
        let center_d: Arc<[f32]> = Arc::from([0.0f32, 0.0, 0.0, 1.0]);
        let v40 = writer.upgrade_version(40);
        let v41 = writer.upgrade_version(41);
        let code_d: Arc<[u8]> = Code::<Vec<u8>>::quantize(&[0.0, 0.0, 0.0, 1.0], &center_d)
            .as_ref()
            .into();
        let delta_d = QuantizedDelta {
            center: center_d.clone(),
            codes: vec![code_d.clone(), code_d.clone()],
            ids: vec![40, 41],
            length: 2,
            versions: vec![v40, v41],
        };
        let cluster_d = writer.spawn(delta_d).expect("spawn D failed");

        let next_cluster_id_after_spawn = writer.next_cluster_id.load(Ordering::Relaxed);

        writer
            .finish(&usearch_provider)
            .await
            .expect("Failed to finish");
        let flusher = Box::pin(writer.commit(&blockfile_provider, &usearch_provider))
            .await
            .expect("Failed to commit");
        let file_ids = Box::pin(flusher.flush()).await.expect("Failed to flush");

        // =======================================================================
        // Reopen and modify
        // =======================================================================
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let usearch_provider = test_usearch_provider(storage.clone());

        let mut writer = QuantizedSpannIndexWriter::<USearchIndex>::open(
            TEST_CLUSTER_BLOCK_SIZE,
            collection_id,
            test_config(),
            TEST_DIMENSION,
            test_distance_function(),
            file_ids,
            None,
            "".to_string(),
            None, // no raw embedding reader needed
            &blockfile_provider,
            &usearch_provider,
        )
        .await
        .expect("Failed to open writer");

        let actual_rotated = writer.rotate(&test_vector);
        assert!(
            writer.distance(&expected_rotated, &actual_rotated) < TEST_EPSILON,
            "Rotation matrix should be preserved across open"
        );

        // --- Cluster A: add more points ---
        writer.load(cluster_a).await.expect("load A failed");
        let v13 = writer.upgrade_version(13);
        let v14 = writer.upgrade_version(14);
        writer.append(cluster_a, 13, v13, code_a.clone());
        writer.append(cluster_a, 14, v14, code_a.clone());

        // --- Cluster B: invalidate points 21 and 23 ---
        writer.load(cluster_b).await.expect("load B failed");
        writer.upgrade_version(21);
        writer.upgrade_version(23);

        // --- Cluster C: invalidate all points ---
        writer.load(cluster_c).await.expect("load C failed");
        writer.upgrade_version(30);
        writer.upgrade_version(31);

        // --- Cluster D: drop ---
        writer.drop(cluster_d).await.expect("drop D failed");

        writer
            .finish(&usearch_provider)
            .await
            .expect("Failed to finish");
        let flusher = Box::pin(writer.commit(&blockfile_provider, &usearch_provider))
            .await
            .expect("Failed to commit");
        let file_ids = Box::pin(flusher.flush()).await.expect("Failed to flush");

        // =======================================================================
        // Verify invariants after reopen
        // =======================================================================
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let usearch_provider = test_usearch_provider(storage.clone());

        let writer = QuantizedSpannIndexWriter::<USearchIndex>::open(
            TEST_CLUSTER_BLOCK_SIZE,
            collection_id,
            test_config(),
            TEST_DIMENSION,
            test_distance_function(),
            file_ids,
            None,
            "".to_string(),
            None,
            &blockfile_provider,
            &usearch_provider,
        )
        .await
        .expect("Failed to open writer after modifications");

        // --- rotation matrix ---
        let actual_rotated = writer.rotate(&test_vector);
        assert!(
            writer.distance(&expected_rotated, &actual_rotated) < TEST_EPSILON,
            "Rotation matrix should be preserved after second open"
        );

        // --- next_cluster_id ---
        assert_eq!(
            writer.next_cluster_id.load(Ordering::Relaxed),
            next_cluster_id_after_spawn,
            "next_cluster_id should be preserved"
        );

        // --- cluster A: exists with 5 points ---
        assert!(
            writer.cluster_deltas.contains_key(&cluster_a),
            "Cluster A should exist"
        );
        {
            let delta = writer.cluster_deltas.get(&cluster_a).unwrap();
            assert_eq!(delta.length, 5, "Cluster A should have 5 points");
        }
        writer.load(cluster_a).await.expect("load A failed");
        {
            let delta = writer.cluster_deltas.get(&cluster_a).unwrap();
            assert_eq!(delta.ids.len(), 5);
            assert!(delta.ids.contains(&10));
            assert!(delta.ids.contains(&11));
            assert!(delta.ids.contains(&12));
            assert!(delta.ids.contains(&13));
            assert!(delta.ids.contains(&14));
        }

        // --- cluster B: exists with 2 points (scrubbed) ---
        assert!(
            writer.cluster_deltas.contains_key(&cluster_b),
            "Cluster B should exist"
        );
        {
            let delta = writer.cluster_deltas.get(&cluster_b).unwrap();
            assert_eq!(
                delta.length, 2,
                "Cluster B should have 2 points after scrub"
            );
        }
        writer.load(cluster_b).await.expect("load B failed");
        {
            let delta = writer.cluster_deltas.get(&cluster_b).unwrap();
            assert_eq!(delta.ids.len(), 2);
            assert!(delta.ids.contains(&20), "Point 20 should survive");
            assert!(delta.ids.contains(&22), "Point 22 should survive");
            assert!(!delta.ids.contains(&21), "Point 21 should be scrubbed");
            assert!(!delta.ids.contains(&23), "Point 23 should be scrubbed");
        }

        // --- cluster C: deleted (all points scrubbed) ---
        assert!(
            !writer.cluster_deltas.contains_key(&cluster_c),
            "Cluster C should not exist (all points invalidated)"
        );

        // --- cluster D: deleted (tombstoned) ---
        assert!(
            !writer.cluster_deltas.contains_key(&cluster_d),
            "Cluster D should not exist (tombstoned)"
        );

        // --- tombstones empty ---
        assert!(
            writer.tombstones.is_empty(),
            "Tombstones should be empty after reopen"
        );

        // --- versions ---
        for id in [10, 11, 12, 13, 14] {
            assert!(
                writer.versions.contains_key(&id),
                "Version for point {} should exist",
                id
            );
        }
        assert_eq!(*writer.versions.get(&20).unwrap(), 1);
        assert_eq!(*writer.versions.get(&22).unwrap(), 1);
        assert_eq!(*writer.versions.get(&21).unwrap(), 2);
        assert_eq!(*writer.versions.get(&23).unwrap(), 2);
        assert_eq!(*writer.versions.get(&30).unwrap(), 2);
        assert_eq!(*writer.versions.get(&31).unwrap(), 2);
        assert!(writer.versions.contains_key(&40));
        assert!(writer.versions.contains_key(&41));

        // --- centroid indexes ---
        let nav_result = writer.navigate(&center_a, 10).expect("navigate failed");
        assert!(nav_result.keys.contains(&cluster_a));

        let nav_result = writer.navigate(&center_b, 10).expect("navigate failed");
        assert!(nav_result.keys.contains(&cluster_b));

        let all_nav = writer
            .navigate(&[0.0, 0.0, 0.0, 0.0], 100)
            .expect("navigate failed");
        assert!(!all_nav.keys.contains(&cluster_c));
        assert!(!all_nav.keys.contains(&cluster_d));
    }

    #[tokio::test]
    async fn test_persist() {
        // === Constants ===
        const SEED: u64 = 42;
        const BATCH_SIZE: usize = 1_000;
        const CHUNK_SIZE: usize = 200;
        const NUM_CYCLES: usize = 4;
        const TOTAL_VECTORS: usize = BATCH_SIZE * NUM_CYCLES; // 4K

        // === Setup ===
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&tmp_dir);
        let collection_id = CollectionUuid::new();

        // Generate all embeddings upfront with fixed seed RNG
        let embeddings = {
            let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
            Arc::new(
                (0..TOTAL_VECTORS)
                    .map(|_| [rng.gen(), rng.gen(), rng.gen(), rng.gen()])
                    .collect::<Vec<_>>(),
            )
        };

        // Create raw embedding blockfile with all 40K embeddings
        let blockfile_provider = test_blockfile_provider(storage.clone());
        let raw_writer = blockfile_provider
            .write::<u32, &DataRecord<'_>>(
                BlockfileWriterOptions::new("".to_string()).ordered_mutations(),
            )
            .await
            .expect("Failed to create raw embedding writer");

        for (id, embedding) in embeddings.iter().enumerate() {
            let record = DataRecord {
                id: "",
                embedding: embedding.as_slice(),
                metadata: None,
                document: None,
            };
            raw_writer
                .set("", id as u32, &record)
                .await
                .expect("Failed to write raw embedding");
        }

        let raw_flusher = raw_writer
            .commit::<u32, &DataRecord<'_>>()
            .await
            .expect("Failed to commit raw embeddings");
        let raw_embedding_id = raw_flusher.id();
        raw_flusher
            .flush::<u32, &DataRecord<'_>>()
            .await
            .expect("Failed to flush raw embeddings");

        // Create initial writer
        let usearch_provider = test_usearch_provider(storage.clone());

        let mut writer = QuantizedSpannIndexWriter::<USearchIndex>::create(
            TEST_CLUSTER_BLOCK_SIZE,
            collection_id,
            test_config(),
            TEST_DIMENSION,
            test_distance_function(),
            None,
            "".to_string(),
            &usearch_provider,
        )
        .await
        .expect("Failed to create writer");

        let mut file_ids;

        // === Cycle Loop ===
        for cycle in 0..NUM_CYCLES {
            let start_id = cycle * BATCH_SIZE;
            let end_id = start_id + BATCH_SIZE;

            // --- Verify previous cycle data (cycles 1-3) ---
            if cycle > 0 {
                // Check version map has all previous IDs
                for id in 0..start_id {
                    assert!(
                        writer.versions.contains_key(&(id as u32)),
                        "Cycle {}: missing ID {} in version map",
                        cycle,
                        id
                    );
                }
            }

            // --- Concurrent insert ---
            let writer_arc = Arc::new(writer);
            let mut handles = vec![];

            for chunk_start in (start_id..end_id).step_by(CHUNK_SIZE) {
                let chunk_end = (chunk_start + CHUNK_SIZE).min(end_id);
                let writer_clone = Arc::clone(&writer_arc);
                let embeddings_clone = Arc::clone(&embeddings);

                handles.push(tokio::spawn(async move {
                    for id in chunk_start..chunk_end {
                        writer_clone
                            .add(id as u32, &embeddings_clone[id])
                            .await
                            .expect("add failed");
                    }
                }));
            }

            for handle in handles {
                handle.await.expect("task panicked");
            }

            writer = Arc::try_unwrap(writer_arc)
                .unwrap_or_else(|_| panic!("Arc still has multiple owners"));

            // --- Pre-commit verification ---
            // For each delta, verify all point versions <= global version
            for delta in writer.cluster_deltas.iter() {
                for (id, ver) in delta.ids.iter().zip(delta.versions.iter()) {
                    let global = *writer
                        .versions
                        .get(id)
                        .expect("ID not found in version map");
                    assert!(
                        *ver <= global,
                        "Cycle {}: version in delta ({}) exceeds global ({}) for ID {}",
                        cycle,
                        ver,
                        global,
                        id
                    );
                }
            }

            // --- Capture rotation before flush ---
            let mut rng = rand::rngs::StdRng::seed_from_u64(SEED + cycle as u64);
            let test_vec = [rng.gen(), rng.gen(), rng.gen(), rng.gen()];
            let expected_rotated = writer.rotate(&test_vec);

            // --- Finish + Commit + Flush ---
            let blockfile_provider = test_blockfile_provider(storage.clone());
            let usearch_provider = test_usearch_provider(storage.clone());

            writer
                .finish(&usearch_provider)
                .await
                .expect("finish failed");
            let flusher = Box::pin(writer.commit(&blockfile_provider, &usearch_provider))
                .await
                .expect("commit failed");
            file_ids = Box::pin(flusher.flush()).await.expect("flush failed");

            // --- Reopen ---
            let blockfile_provider = test_blockfile_provider(storage.clone());
            let usearch_provider = test_usearch_provider(storage.clone());

            let raw_reader = blockfile_provider
                .read::<u32, DataRecord<'static>>(BlockfileReaderOptions::new(
                    raw_embedding_id,
                    "".to_string(),
                ))
                .await
                .expect("Failed to open raw embedding reader");

            writer = QuantizedSpannIndexWriter::<USearchIndex>::open(
                TEST_CLUSTER_BLOCK_SIZE,
                collection_id,
                test_config(),
                TEST_DIMENSION,
                test_distance_function(),
                file_ids.clone(),
                None,
                "".to_string(),
                Some(raw_reader),
                &blockfile_provider,
                &usearch_provider,
            )
            .await
            .expect("Failed to reopen writer");

            // --- Verify rotation matrix consistency after reopen ---
            let actual_rotated = writer.rotate(&test_vec);
            assert!(
                writer.distance(&expected_rotated, &actual_rotated) < TEST_EPSILON,
                "Cycle {}: rotation matrix changed after persistence",
                cycle
            );
        }

        // === Final Verification ===
        assert_eq!(
            writer.versions.len(),
            TOTAL_VECTORS,
            "Expected {} IDs in version map, got {}",
            TOTAL_VECTORS,
            writer.versions.len()
        );

        // Verify all IDs are present in version map
        for id in 0..TOTAL_VECTORS {
            assert!(
                writer.versions.contains_key(&(id as u32)),
                "Final: missing ID {} in version map",
                id
            );
        }

        // --- Load all clusters and verify delta consistency ---
        let cluster_ids = writer
            .cluster_deltas
            .iter()
            .map(|e| *e.key())
            .collect::<Vec<_>>();

        for cluster_id in &cluster_ids {
            // Get length before load (from blockfile metadata, already set during open)
            let length_before = writer.cluster_deltas.get(cluster_id).unwrap().length;

            writer.load(*cluster_id).await.expect("load failed");

            // After load, verify ids/codes/versions lengths match
            let delta = writer.cluster_deltas.get(cluster_id).unwrap();
            assert_eq!(
                delta.ids.len(),
                length_before,
                "Cluster {}: loaded ids length ({}) != expected length ({})",
                cluster_id,
                delta.ids.len(),
                length_before
            );
            assert_eq!(
                delta.codes.len(),
                length_before,
                "Cluster {}: loaded codes length mismatch",
                cluster_id
            );
            assert_eq!(
                delta.versions.len(),
                length_before,
                "Cluster {}: loaded versions length mismatch",
                cluster_id
            );

            // Verify all versions in delta <= global version
            for (id, ver) in delta.ids.iter().zip(delta.versions.iter()) {
                let global = *writer.versions.get(id).expect("missing from version map");
                assert!(
                    *ver <= global,
                    "Cluster {}: version in delta ({}) exceeds global ({}) for ID {}",
                    cluster_id,
                    ver,
                    global,
                    id
                );
            }
        }

        // --- Verify each vector has at least one up-to-date copy ---
        for id in 0..TOTAL_VECTORS {
            let id = id as u32;
            let global = *writer.versions.get(&id).expect("missing ID");

            let has_current_copy = writer.cluster_deltas.iter().any(|delta| {
                delta
                    .ids
                    .iter()
                    .zip(delta.versions.iter())
                    .any(|(did, dver)| *did == id && *dver == global)
            });

            assert!(
                has_current_copy,
                "ID {} has no up-to-date copy in any cluster (global version: {})",
                id, global
            );
        }

        // --- Verify raw centroid index contains all cluster IDs ---
        for cluster_id in &cluster_ids {
            let result = writer.raw_centroid.get(*cluster_id);
            assert!(
                result.is_ok() && result.unwrap().is_some(),
                "Cluster {} not found in raw_centroid index",
                cluster_id
            );
        }
    }
}
