//! Ground truth loading utilities.

use std::fs::File;
use std::io;
use std::path::Path;

use arrow::array::{Array, Float32Array, ListArray, UInt32Array, UInt64Array};
use chroma_distance::DistanceFunction;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::Query;

/// Number of neighbors in ground truth.
pub const K: usize = 100;

/// Chunk size for progressive ground truth (1M vectors).
pub const CHUNK_SIZE: usize = 1_000_000;

/// Queries per million vectors.
pub const QUERIES_PER_MILLION: usize = 1_000;

/// Compute the expected number of queries for a dataset of given size.
pub fn expected_num_queries(data_len: usize) -> usize {
    let num_chunks = data_len.div_ceil(CHUNK_SIZE);
    let mut total = 0;
    for chunk_idx in 0..num_chunks {
        let chunk_start = chunk_idx * CHUNK_SIZE;
        let chunk_end = (chunk_start + CHUNK_SIZE).min(data_len);
        let chunk_size = chunk_end - chunk_start;
        let num_queries = (chunk_size * QUERIES_PER_MILLION / CHUNK_SIZE).max(1);
        total += num_queries;
    }
    total
}

/// Load ground truth from parquet file for the given distance function.
///
/// Expected schema:
/// - `query_vector`: list<f32>
/// - `max_vector_id`: u64
/// - `neighbors_l2`: list<u32>
/// - `neighbors_ip`: list<u32>
/// - `neighbors_cosine`: list<u32>
pub fn load(path: &Path, distance_function: DistanceFunction) -> io::Result<Vec<Query>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let reader = builder
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let neighbor_col_name = match distance_function {
        DistanceFunction::Cosine => "neighbors_cosine",
        DistanceFunction::Euclidean => "neighbors_l2",
        DistanceFunction::InnerProduct => "neighbors_ip",
    };

    let mut queries = Vec::new();

    for batch in reader {
        let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let query_col = batch.column_by_name("query_vector").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing query_vector column")
        })?;
        let max_vector_id_col = batch.column_by_name("max_vector_id").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing max_vector_id column")
        })?;
        let neighbor_col = batch.column_by_name(neighbor_col_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing {} column", neighbor_col_name),
            )
        })?;

        let query_list = query_col
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "query_vector not a list"))?;
        let max_vector_id_array = max_vector_id_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "max_vector_id not u64"))?;
        let neighbor_list = neighbor_col
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} not a list", neighbor_col_name),
                )
            })?;

        for i in 0..batch.num_rows() {
            let query_values = query_list.value(i);
            let query_array = query_values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "query values not f32")
                })?;
            let vector: Vec<f32> = query_array.values().to_vec();

            let max_vector_id = max_vector_id_array.value(i);

            let neighbor_values = neighbor_list.value(i);
            let neighbor_array = neighbor_values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "neighbor values not u32")
                })?;
            let neighbors: Vec<u32> = neighbor_array.values().to_vec();

            queries.push(Query {
                vector,
                neighbors,
                max_vector_id,
            });
        }
    }

    Ok(queries)
}

/// Check if ground truth cache exists.
pub fn exists(path: &Path) -> bool {
    path.exists()
}
