//! SIFT1M dataset: 1M vectors, 128 dimensions, Euclidean distance.
//!
//! Source: open-vdb/sift-128-euclidean on HuggingFace.
//! Train: 1M vectors (idx, emb). Test: 10K queries. Neighbors: 100-NN ground truth (L2 only).

use std::fs::File;
use std::io;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, Int64Array, ListArray};
use arrow::datatypes::ArrowNativeType;
use chroma_distance::DistanceFunction;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{Dataset, LazyShardLoader, Query};

const REPO_ID: &str = "open-vdb/sift-128-euclidean";
pub const DIMENSION: usize = 128;
pub const DATA_LEN: usize = 1_000_000;
const K: usize = 100;

pub struct Sift {
    loader: LazyShardLoader,
}

impl Sift {
    pub async fn load() -> io::Result<Self> {
        println!("Loading SIFT-1M from HuggingFace Hub...");
        let shard_names = vec![
            "train/train-00001-of-00001.parquet".to_string(),
            "test/test-00001-of-00001.parquet".to_string(),
            "neighbors/neighbors-vector-emb-pk-idx-expr-None-metric-l2.parquet".to_string(),
        ];
        let loader = LazyShardLoader::new(REPO_ID, shard_names)?;
        Ok(Self { loader })
    }

    fn load_vectors_from_parquet(
        path: &std::path::Path,
        offset: usize,
        limit: usize,
        total: usize,
    ) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        let end = (offset + limit).min(total);
        if offset >= end {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reader = builder
            .with_batch_size(10_000)
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut result = Vec::with_capacity(end - offset);
        let mut global_idx = 0usize;

        for batch in reader {
            if result.len() >= limit {
                break;
            }

            let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let emb_col = batch
                .column_by_name("emb")
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing emb column"))?;
            let list_array = emb_col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "emb column is not a list")
                })?;

            let offsets = list_array.offsets();
            let inner = list_array
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "emb values not f32")
                })?;

            for i in 0..list_array.len() {
                if global_idx < offset {
                    global_idx += 1;
                    continue;
                }
                if result.len() >= limit {
                    break;
                }

                let start = offsets[i].as_usize();
                let end_off = offsets[i + 1].as_usize();
                let vec: Arc<[f32]> = Arc::from(&inner.values()[start..end_off]);

                result.push((global_idx as u32, vec));
                global_idx += 1;
            }
        }

        Ok(result)
    }
}

impl Dataset for Sift {
    fn name(&self) -> &str {
        "sift-1m"
    }

    fn dimension(&self) -> usize {
        DIMENSION
    }

    fn data_len(&self) -> usize {
        DATA_LEN
    }

    fn k(&self) -> usize {
        K
    }

    fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        let train_path = self.loader.get(0)?;
        Self::load_vectors_from_parquet(&train_path, offset, limit, DATA_LEN)
    }

    fn queries(&self, distance_function: DistanceFunction) -> io::Result<Vec<Query>> {
        if distance_function != DistanceFunction::Euclidean {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SIFT dataset only has L2 ground truth",
            ));
        }

        let test_path = self.loader.get(1)?;
        let neighbors_path = self.loader.get(2)?;

        let test_file = File::open(&test_path)?;
        let test_builder = ParquetRecordBatchReaderBuilder::try_new(test_file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let test_reader = test_builder
            .with_batch_size(10_000)
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut query_vectors: Vec<Vec<f32>> = Vec::new();
        for batch in test_reader {
            let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let emb_col = batch
                .column_by_name("emb")
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing emb column"))?;
            let list_array = emb_col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "emb column is not a list")
                })?;
            let offsets = list_array.offsets();
            let inner = list_array
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "emb values not f32")
                })?;

            for i in 0..list_array.len() {
                let start = offsets[i].as_usize();
                let end_off = offsets[i + 1].as_usize();
                query_vectors.push(inner.values()[start..end_off].to_vec());
            }
        }

        let nb_file = File::open(&neighbors_path)?;
        let nb_builder = ParquetRecordBatchReaderBuilder::try_new(nb_file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let nb_reader = nb_builder
            .with_batch_size(10_000)
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut all_neighbors: Vec<Vec<u32>> = Vec::new();
        for batch in nb_reader {
            let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let nb_col = batch.column_by_name("neighbors_id").ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing neighbors_id column")
            })?;
            let list_array = nb_col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "neighbors_id column is not a list",
                    )
                })?;
            let offsets = list_array.offsets();
            let inner = list_array.values();

            let int64_arr = inner.as_any().downcast_ref::<Int64Array>();

            for i in 0..list_array.len() {
                let start = offsets[i].as_usize();
                let end_off = offsets[i + 1].as_usize();

                let neighbors: Vec<u32> = if let Some(arr) = int64_arr {
                    arr.values()[start..end_off]
                        .iter()
                        .map(|&v| v as u32)
                        .collect()
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "neighbors_id values not int64",
                    ));
                };
                all_neighbors.push(neighbors);
            }
        }

        if query_vectors.len() != all_neighbors.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "query/neighbor count mismatch: {} queries vs {} neighbor lists",
                    query_vectors.len(),
                    all_neighbors.len()
                ),
            ));
        }

        let queries: Vec<Query> = query_vectors
            .into_iter()
            .zip(all_neighbors)
            .map(|(vector, neighbors)| Query {
                vector,
                neighbors,
                max_vector_id: DATA_LEN as u64,
            })
            .collect();

        Ok(queries)
    }
}
