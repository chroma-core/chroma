//! DBPedia OpenAI embeddings dataset: ~1M vectors, 1536 dimensions.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, Float64Array, ListArray};
use arrow::datatypes::ArrowNativeType;
use chroma_distance::DistanceFunction;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::ground_truth;
use super::Query;

const REPO_ID: &str = "KShivendu/dbpedia-entities-openai-1M";
const NUM_SHARDS: usize = 26;
pub const DIMENSION: usize = 1536;
pub const DATA_LEN: usize = 1_000_000;
const COLUMN: &str = "openai";

const SHARD_FILES: [&str; NUM_SHARDS] = [
    "data/train-00000-of-00026-3c7b99d1c7eda36e.parquet",
    "data/train-00001-of-00026-2b24035a6390fdcb.parquet",
    "data/train-00002-of-00026-b05ce48965853dad.parquet",
    "data/train-00003-of-00026-d116c3c239aa7895.parquet",
    "data/train-00004-of-00026-5c2bcfc39e1019cd.parquet",
    "data/train-00005-of-00026-b674673d537c6296.parquet",
    "data/train-00006-of-00026-56ad5cf7824f3ccd.parquet",
    "data/train-00007-of-00026-ff1a7a93f5f19199.parquet",
    "data/train-00008-of-00026-90ec96c23a661926.parquet",
    "data/train-00009-of-00026-8bcdf317ec2e082c.parquet",
    "data/train-00010-of-00026-eb06930da76c47f3.parquet",
    "data/train-00011-of-00026-2df07d31f0aa1838.parquet",
    "data/train-00012-of-00026-f2965048cabd6c86.parquet",
    "data/train-00013-of-00026-2036414050f9f044.parquet",
    "data/train-00014-of-00026-820dc150346715fc.parquet",
    "data/train-00015-of-00026-c3b25654a256c528.parquet",
    "data/train-00016-of-00026-7deff0f4bdb4c24d.parquet",
    "data/train-00017-of-00026-ab8aaded21783fd6.parquet",
    "data/train-00018-of-00026-27b906a8a01de2e0.parquet",
    "data/train-00019-of-00026-9f5371122f68f762.parquet",
    "data/train-00020-of-00026-e6ff711af402609d.parquet",
    "data/train-00021-of-00026-34a5d25f74b06b5f.parquet",
    "data/train-00022-of-00026-61a9e6318ee525b5.parquet",
    "data/train-00023-of-00026-2f7b85c21ea5f957.parquet",
    "data/train-00024-of-00026-c8000d2c489222ab.parquet",
    "data/train-00025-of-00026-769064ea76815001.parquet",
];

fn cache_dir() -> PathBuf {
    dirs::home_dir()
        .expect("failed to get home directory")
        .join(".cache/dbpedia")
}

fn gt_path() -> PathBuf {
    cache_dir().join("ground_truth.parquet")
}

/// DBPedia dataset handle.
pub struct DbPedia {
    shard_paths: Vec<PathBuf>,
}

impl DbPedia {
    /// Load DBPedia dataset from HuggingFace Hub.
    /// Requires ground truth to be precomputed at ~/.cache/dbpedia/ground_truth.parquet
    pub async fn load() -> io::Result<Self> {
        // Check ground truth exists before downloading shards
        if !ground_truth::exists(&gt_path()) {
            return Err(io::Error::other(format!(
                "Ground truth not found at {}.\n  \
                 Run: python sphroma/scripts/compute_ground_truth.py --dataset dbpedia",
                gt_path().display()
            )));
        }

        println!("Loading DBPedia from HuggingFace Hub...");

        let api = hf_hub::api::tokio::Api::new().map_err(io::Error::other)?;
        let repo = api.dataset(REPO_ID.to_string());

        let mut shard_paths = Vec::with_capacity(NUM_SHARDS);
        for filename in SHARD_FILES.iter() {
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

    /// Load ground truth queries for the given distance function.
    pub fn queries(&self, distance_function: DistanceFunction) -> io::Result<Vec<Query>> {
        ground_truth::load(&gt_path(), distance_function)
    }

    pub fn dimension(&self) -> usize {
        DIMENSION
    }

    pub fn data_len(&self) -> usize {
        DATA_LEN
    }

    pub fn k(&self) -> usize {
        ground_truth::K
    }
}
