//! Wikipedia EN with Cohere embed-multilingual-v3 embeddings: ~41.5M vectors, 1024 dimensions.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, Float64Array, ListArray};
use arrow::datatypes::ArrowNativeType;
use chroma_distance::DistanceFunction;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{ground_truth, Dataset, Query};

const REPO_ID: &str = "Cohere/wikipedia-2023-11-embed-multilingual-v3";
const NUM_SHARDS: usize = 415;
pub const DIMENSION: usize = 1024;
pub const DATA_LEN: usize = 41_488_110;
const COLUMN: &str = "emb";

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

/// Wikipedia EN dataset handle.
pub struct Wikipedia {
    shard_paths: Vec<PathBuf>,
}

impl Wikipedia {
    /// Load Wikipedia EN dataset from HuggingFace Hub.
    /// Requires ground truth to be precomputed at ~/.cache/wikipedia_en/ground_truth.parquet
    pub async fn load() -> io::Result<Self> {
        // Check ground truth exists before downloading shards
        if !ground_truth::exists(&gt_path()) {
            return Err(io::Error::other(format!(
                "Ground truth not found at {}.\n  \
                 Run: python sphroma/scripts/compute_ground_truth.py --dataset wikipedia",
                gt_path().display()
            )));
        }

        println!("Loading Wikipedia EN from HuggingFace Hub...");

        let api = hf_hub::api::tokio::Api::new().map_err(io::Error::other)?;
        let repo = api.dataset(REPO_ID.to_string());

        let shard_files = shard_files();
        let mut shard_paths = Vec::with_capacity(NUM_SHARDS);
        for filename in shard_files.iter() {
            let path = repo.get(filename).await.map_err(io::Error::other)?;
            shard_paths.push(path);
        }

        Ok(Self { shard_paths })
    }

    /// Load vectors in range [offset, offset+limit).
    /// Returns (global_id, embedding) pairs.
    pub fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        let end = (offset + limit).min(DATA_LEN);
        if offset >= end {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(end - offset);
        let mut global_idx = 0usize;
        let mut collected = 0usize;

        for shard_path in &self.shard_paths {
            if collected >= limit || global_idx >= end {
                break;
            }

            let file = File::open(shard_path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let num_rows = builder.metadata().file_metadata().num_rows() as usize;

            // Skip shards entirely before our range
            if global_idx + num_rows <= offset {
                global_idx += num_rows;
                continue;
            }

            let reader = builder
                .with_batch_size(10_000)
                .build()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            for batch in reader {
                if collected >= limit {
                    break;
                }

                let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let col_idx = batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == COLUMN)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "column not found")
                    })?;

                let col = batch.column(col_idx);
                let list_array = col.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "column is not a list")
                })?;

                let offsets = list_array.offsets();
                let inner = list_array.values();

                for i in 0..list_array.len() {
                    if list_array.is_null(i) {
                        global_idx += 1;
                        continue;
                    }

                    // Skip if before offset
                    if global_idx < offset {
                        global_idx += 1;
                        continue;
                    }

                    // Stop if we've collected enough
                    if collected >= limit {
                        break;
                    }

                    let start = offsets[i].as_usize();
                    let end_off = offsets[i + 1].as_usize();

                    let vec: Arc<[f32]> = if let Some(f32_arr) =
                        inner.as_any().downcast_ref::<Float32Array>()
                    {
                        Arc::from(&f32_arr.values()[start..end_off])
                    } else if let Some(f64_arr) = inner.as_any().downcast_ref::<Float64Array>() {
                        let values: Vec<f32> = f64_arr.values()[start..end_off]
                            .iter()
                            .map(|&v| v as f32)
                            .collect();
                        Arc::from(values)
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unsupported array type",
                        ));
                    };

                    result.push((global_idx as u32, vec));
                    global_idx += 1;
                    collected += 1;
                }
            }
        }

        Ok(result)
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
        ground_truth::load(&gt_path(), distance_function)
    }
}
