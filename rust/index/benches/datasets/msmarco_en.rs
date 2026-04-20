//! MSMARCO v2.1 with Cohere Embed English v3 embeddings: ~113.5M passages, 1024 dimensions.
//! Includes 1,677 TREC-DL queries with precomputed brute-force top-1k ground truth.
//!
//! Dataset: CohereLabs/msmarco-v2.1-embed-english-v3
//! Shards: passages_parquet/msmarco_v2.1_doc_segmented_{00..59}.parquet
//! Queries: queries_parquet/queries.parquet

use std::fs::File;
use std::io;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, Float64Array, Int64Array, ListArray};
use arrow::datatypes::ArrowNativeType;
use chroma_distance::DistanceFunction;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{Dataset, LazyShardLoader, Query};

const REPO_ID: &str = "CohereLabs/msmarco-v2.1-embed-english-v3";
const NUM_SHARDS: usize = 60;
pub const DIMENSION: usize = 1024;
pub const DATA_LEN: usize = 113_520_750;
const COLUMN: &str = "emb";
const QUERY_FILE: &str = "queries_parquet/queries.parquet";

fn shard_files() -> Vec<String> {
    (0..NUM_SHARDS)
        .map(|i| format!("passages_parquet/msmarco_v2.1_doc_segmented_{:02}.parquet", i))
        .collect()
}

pub struct MsMarcoEn {
    loader: LazyShardLoader,
    query_loader: LazyShardLoader,
}

impl MsMarcoEn {
    pub async fn load() -> io::Result<Self> {
        println!("Loading MSMARCO v2.1 English from HuggingFace Hub...");
        let loader = LazyShardLoader::new(REPO_ID, shard_files())?;
        let query_loader = LazyShardLoader::new(REPO_ID, vec![QUERY_FILE.to_string()])?;
        Ok(Self {
            loader,
            query_loader,
        })
    }

    pub fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        let end = (offset + limit).min(DATA_LEN);
        if offset >= end {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(end - offset);
        let mut global_idx = 0usize;
        let mut collected = 0usize;

        for shard_idx in 0..self.loader.num_shards() {
            if collected >= limit || global_idx >= end {
                break;
            }

            let shard_path = self.loader.get(shard_idx)?;
            let file = File::open(&shard_path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let num_rows = builder.metadata().file_metadata().num_rows() as usize;

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
                        io::Error::new(io::ErrorKind::InvalidData, "emb column not found")
                    })?;

                let col = batch.column(col_idx);
                let list_array = col.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "emb column is not a list")
                })?;

                let offsets = list_array.offsets();
                let inner = list_array.values();

                for i in 0..list_array.len() {
                    if list_array.is_null(i) {
                        global_idx += 1;
                        continue;
                    }

                    if global_idx < offset {
                        global_idx += 1;
                        continue;
                    }

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
                            "unsupported embedding array type",
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

    /// Load queries from the precomputed queries parquet.
    /// Columns: emb (query vector), top1k_offsets (neighbor passage offsets).
    /// The top1k_offsets are sequential integer passage IDs matching our global_idx ordering.
    fn load_queries(&self) -> io::Result<Vec<Query>> {
        let query_path = self.query_loader.get(0)?;
        let file = File::open(&query_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let reader = builder
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut queries = Vec::new();

        for batch in reader {
            let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let emb_col = batch.column_by_name("emb").ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing emb column in queries")
            })?;
            let offsets_col = batch.column_by_name("top1k_offsets").ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing top1k_offsets column in queries",
                )
            })?;

            let emb_list = emb_col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "emb column is not a list")
                })?;
            let offsets_list = offsets_col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "top1k_offsets column is not a list",
                    )
                })?;

            for i in 0..batch.num_rows() {
                let emb_values = emb_list.value(i);
                let vector: Vec<f32> =
                    if let Some(f32_arr) = emb_values.as_any().downcast_ref::<Float32Array>() {
                        f32_arr.values().to_vec()
                    } else if let Some(f64_arr) =
                        emb_values.as_any().downcast_ref::<Float64Array>()
                    {
                        f64_arr.values().iter().map(|&v| v as f32).collect()
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "query emb values not f32 or f64",
                        ));
                    };

                let offset_values = offsets_list.value(i);
                let neighbors: Vec<u32> =
                    if let Some(i64_arr) = offset_values.as_any().downcast_ref::<Int64Array>() {
                        i64_arr.values().iter().map(|&v| v as u32).collect()
                    } else if let Some(i32_arr) = offset_values
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                    {
                        i32_arr.values().iter().map(|&v| v as u32).collect()
                    } else if let Some(u64_arr) = offset_values
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                    {
                        u64_arr.values().iter().map(|&v| v as u32).collect()
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "top1k_offsets values not i64, i32, or u64",
                        ));
                    };

                queries.push(Query {
                    vector,
                    neighbors,
                    max_vector_id: DATA_LEN as u64,
                });
            }
        }

        println!(
            "  Loaded {} queries with top-1k ground truth from HuggingFace",
            queries.len(),
        );
        Ok(queries)
    }
}

impl Dataset for MsMarcoEn {
    fn name(&self) -> &str {
        "msmarco-v2.1-en"
    }

    fn dimension(&self) -> usize {
        DIMENSION
    }

    fn data_len(&self) -> usize {
        DATA_LEN
    }

    fn k(&self) -> usize {
        100
    }

    fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        MsMarcoEn::load_range(self, offset, limit)
    }

    fn queries(&self, _distance_function: DistanceFunction) -> io::Result<Vec<Query>> {
        self.load_queries()
    }
}
