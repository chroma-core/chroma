//! Select the physical rows and embedding column needed by a checkpoint.

use std::fs::File;
use std::io;

use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;

/// Avoid decoding unrelated columns and earlier row groups in the same shard.
pub(super) fn embedding_reader(
    builder: ParquetRecordBatchReaderBuilder<File>,
    column: &str,
    offset: usize,
    limit: usize,
) -> io::Result<ParquetRecordBatchReader> {
    let num_rows = builder.metadata().file_metadata().num_rows() as usize;
    let column_idx = builder
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == column)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "column not found"))?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), [column_idx]);
    let shard_end = offset.saturating_add(limit).min(num_rows);
    let mut row_group_start = 0usize;
    let mut selected_start = None;
    let row_groups: Vec<_> = builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .filter_map(|(idx, group)| {
            let start = row_group_start;
            row_group_start += group.num_rows() as usize;
            if start < shard_end && row_group_start > offset {
                selected_start.get_or_insert(start);
                Some(idx)
            } else {
                None
            }
        })
        .collect();
    // The decoder counts from the first selected row group, not the file start.
    let decode_offset = offset - selected_start.unwrap_or(offset);
    builder
        .with_projection(projection)
        .with_row_groups(row_groups)
        .with_offset(decode_offset)
        .with_limit(shard_end - offset)
        .with_batch_size(10_000)
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
