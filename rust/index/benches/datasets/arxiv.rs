//! Arxiv abstracts with mxbai-embed-large-v1 embeddings: ~3M vectors, 1024 dimensions.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, Float64Array, ListArray};
use arrow::datatypes::ArrowNativeType;
use chroma_distance::DistanceFunction;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{ground_truth, Dataset, Query};

const REPO_ID: &str = "bluuebunny/arxiv_abstract_embedding_mxbai_large_v1_milvus";
const NUM_SHARDS: usize = 35;
pub const DIMENSION: usize = 1024;
pub const DATA_LEN: usize = 2_922_184;
const COLUMN: &str = "vector";

const SHARD_FILES: [&str; NUM_SHARDS] = [
    "data/1991.parquet",
    "data/1992.parquet",
    "data/1993.parquet",
    "data/1994.parquet",
    "data/1995.parquet",
    "data/1996.parquet",
    "data/1997.parquet",
    "data/1998.parquet",
    "data/1999.parquet",
    "data/2000.parquet",
    "data/2001.parquet",
    "data/2002.parquet",
    "data/2003.parquet",
    "data/2004.parquet",
    "data/2005.parquet",
    "data/2006.parquet",
    "data/2007.parquet",
    "data/2008.parquet",
    "data/2009.parquet",
    "data/2010.parquet",
    "data/2011.parquet",
    "data/2012.parquet",
    "data/2013.parquet",
    "data/2014.parquet",
    "data/2015.parquet",
    "data/2016.parquet",
    "data/2017.parquet",
    "data/2018.parquet",
    "data/2019.parquet",
    "data/2020.parquet",
    "data/2021.parquet",
    "data/2022.parquet",
    "data/2023.parquet",
    "data/2024.parquet",
    "data/2025.parquet",
];

fn cache_dir() -> PathBuf {
    dirs::home_dir()
        .expect("failed to get home directory")
        .join(".cache/arxiv_mxbai")
}

fn gt_path() -> PathBuf {
    cache_dir().join("ground_truth.parquet")
}

/// Arxiv dataset handle.
pub struct Arxiv {
    shard_paths: Vec<PathBuf>,
}

impl Arxiv {
    /// Load Arxiv dataset from HuggingFace Hub.
    /// Requires ground truth to be precomputed at ~/.cache/arxiv_mxbai/ground_truth.parquet
    pub async fn load() -> io::Result<Self> {
        // Check ground truth exists before downloading shards
        if !ground_truth::exists(&gt_path()) {
            return Err(io::Error::other(format!(
                "Ground truth not found at {}.\n  \
                 Run: python sphroma/scripts/compute_ground_truth.py --dataset arxiv",
                gt_path().display()
            )));
        }

        println!("Loading Arxiv from HuggingFace Hub...");

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
}

impl Dataset for Arxiv {
    fn name(&self) -> &str {
        "arxiv"
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
        Arxiv::load_range(self, offset, limit)
    }

    fn queries(&self, distance_function: DistanceFunction) -> io::Result<Vec<Query>> {
        ground_truth::load(&gt_path(), distance_function)
    }
}
