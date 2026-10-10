//! Decode independent Wikipedia files with bounded concurrency and ordered output.

use std::{fs::File, io, path::PathBuf, sync::Arc, thread};

use arrow::array::{Array, Float32Array, Float64Array, ListArray};
use arrow::datatypes::ArrowNativeType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::parquet_range;

pub(super) struct ShardRange {
    pub path: PathBuf,
    pub first_id: usize,
    pub offset: usize,
    pub len: usize,
}

type Vectors = Vec<(u32, Arc<[f32]>)>;

pub(super) fn load_ranges(ranges: &[ShardRange], workers: usize) -> io::Result<Vectors> {
    let mut result = Vec::with_capacity(ranges.iter().map(|r| r.len).sum());
    // At most four decoders retain temporary Arrow batches. Join in file order
    // so scheduling cannot change vector IDs, float values, or output order.
    for group in ranges.chunks(workers.clamp(1, 4)) {
        if group.len() == 1 {
            result.extend(decode(&group[0])?);
            continue;
        }
        thread::scope(|scope| -> io::Result<()> {
            let handles: Vec<_> = group
                .iter()
                .map(|range| scope.spawn(move || decode(range)))
                .collect();
            // Join every worker, including after an error, before returning.
            let decoded: Vec<_> = handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .unwrap_or_else(|_| Err(io::Error::other("Wikipedia decoder panicked")))
                })
                .collect();
            for vectors in decoded {
                result.extend(vectors?);
            }
            Ok(())
        })?;
    }
    Ok(result)
}

fn decode(range: &ShardRange) -> io::Result<Vectors> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&range.path)?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let reader = parquet_range::embedding_reader(builder, "emb", range.offset, range.len)?;
    let mut result = Vec::with_capacity(range.len);
    let mut id = range.first_id;
    for batch in reader {
        let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "column is not a list"))?;
        let inner = lists.values();
        for i in 0..lists.len() {
            if !lists.is_null(i) {
                let start = lists.offsets()[i].as_usize();
                let end = lists.offsets()[i + 1].as_usize();
                let vector: Arc<[f32]> =
                    if let Some(values) = inner.as_any().downcast_ref::<Float32Array>() {
                        Arc::from(&values.values()[start..end])
                    } else if let Some(values) = inner.as_any().downcast_ref::<Float64Array>() {
                        let floats: Vec<f32> = values.values()[start..end]
                            .iter()
                            .map(|&v| v as f32)
                            .collect();
                        Arc::from(floats)
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unsupported array type",
                        ));
                    };
                result.push((id as u32, vector));
            }
            // Null rows still occupy physical IDs in the source dataset.
            id += 1;
        }
    }
    Ok(result)
}
