//! Wikipedia EN with Cohere embed-multilingual-v3 embeddings: ~41.5M vectors, 1024 dimensions.
//!
//! Shards are downloaded lazily: only the parquet files actually needed by
//! `load_range()` are fetched from HuggingFace, keeping disk usage proportional
//! to the number of vectors requested rather than the full 41.5M-vector dataset.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{ground_truth, Dataset, LazyShardLoader, Query};

const REPO_ID: &str = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3";
const NUM_SHARDS: usize = 415;
pub const DIMENSION: usize = 1024;
pub const DATA_LEN: usize = 41_488_110;
#[path = "wikipedia_shards.rs"]
mod shards;

fn shard_files() -> Vec<String> {
    (0..NUM_SHARDS)
        .map(|i| format!("en/{:04}.parquet", i))
        .collect()
}

fn cache_dir() -> PathBuf {
    dirs::home_dir()
        .expect("failed to get home directory")
        .join(".cache/wikipedia_en")
}

fn gt_path() -> PathBuf {
    cache_dir().join("ground_truth.parquet")
}

/// Wikipedia EN dataset handle. Shards are downloaded on demand.
pub struct Wikipedia {
    loader: LazyShardLoader,
}

impl Wikipedia {
    /// Prepare Wikipedia EN dataset handle (no shard downloads happen here).
    /// Ground truth is optional when the benchmark computes it from the indexed slice.
    pub async fn load() -> io::Result<Self> {
        if !ground_truth::exists(&gt_path()) {
            println!(
                "Note: ground truth not found at {}. Use --brute-force-gt for recall evaluation.",
                gt_path().display()
            );
        }

        println!("Loading Wikipedia EN from HuggingFace Hub...");
        let loader = LazyShardLoader::new(REPO_ID, shard_files())?;
        Ok(Self { loader })
    }

    /// Load vectors in range [offset, offset+limit).
    /// Only the shards overlapping the requested range are downloaded.
    pub fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        let end = offset.saturating_add(limit).min(DATA_LEN);
        if offset >= end {
            return Ok(Vec::new());
        }

        // Resolve downloads sequentially; only local decoding runs concurrently.
        let mut ranges = Vec::new();
        let mut global_idx = 0usize;
        for shard_idx in 0..self.loader.num_shards() {
            if global_idx >= end {
                break;
            }
            let path = self.loader.get(shard_idx)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let rows = builder.metadata().file_metadata().num_rows() as usize;
            let shard_end = global_idx + rows;
            if shard_end > offset {
                let local_offset = offset.saturating_sub(global_idx);
                ranges.push(shards::ShardRange {
                    path,
                    first_id: global_idx + local_offset,
                    offset: local_offset,
                    len: end.min(shard_end) - (global_idx + local_offset),
                });
            }
            global_idx = shard_end;
        }
        let workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(4);
        shards::load_ranges(&ranges, workers)
    }
}

impl Dataset for Wikipedia {
    fn name(&self) -> &str {
        "wikipedia-en"
    }

    fn dimension(&self) -> usize {
        DIMENSION
    }

    fn data_len(&self) -> usize {
        DATA_LEN
    }

    fn k(&self) -> usize {
        ground_truth::K
    }

    fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        Wikipedia::load_range(self, offset, limit)
    }

    fn queries(&self, distance_function: DistanceFunction) -> io::Result<Vec<Query>> {
        if ground_truth::exists(&gt_path()) {
            ground_truth::load(&gt_path(), distance_function)
        } else {
            Ok(Vec::new())
        }
    }
}
