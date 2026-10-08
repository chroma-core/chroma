//! Check that checkpoint reads preserve physical row offsets and vector values.
#[path = "../benches/datasets/parquet_range.rs"]
mod parquet_range;

use std::fs::File;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, Float64Array, ListArray, StringArray};
use arrow::datatypes::{Float32Type, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn check_ranges(embeddings: ListArray) {
    let text = StringArray::from(vec!["unused"; embeddings.len()]);
    let schema = Arc::new(Schema::new(vec![
        arrow::datatypes::Field::new("text", text.data_type().clone(), false),
        arrow::datatypes::Field::new("emb", embeddings.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(text), Arc::new(embeddings.clone())],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(3)
        .build();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    for (offset, limit) in [(0, 2), (2, 4), (4, 3), (7, 1), (0, 0), (3, 0)] {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(file.path()).unwrap()).unwrap();
        let reader = parquet_range::embedding_reader(builder, "emb", offset, limit).unwrap();
        let mut index = offset;
        for batch in reader {
            let batch = batch.unwrap();
            assert_eq!(batch.num_columns(), 1);
            let list = batch
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..list.len() {
                assert_eq!(list.is_null(row), embeddings.is_null(index));
                if !list.is_null(row) {
                    let actual = list.value(row);
                    let expected = embeddings.value(index);
                    if let Some(actual) = actual.as_any().downcast_ref::<Float32Array>() {
                        let expected = expected.as_any().downcast_ref::<Float32Array>().unwrap();
                        assert_eq!(actual.values(), expected.values());
                    } else {
                        let actual = actual.as_any().downcast_ref::<Float64Array>().unwrap();
                        let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
                        assert_eq!(actual.values(), expected.values());
                    }
                }
                index += 1;
            }
        }
        assert_eq!(index, offset + limit);
    }
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(file.path()).unwrap()).unwrap();
    assert!(parquet_range::embedding_reader(builder, "missing", 0, 1).is_err());
}

#[test]
fn float32_ranges_preserve_offsets_across_row_groups_and_nulls() {
    check_ranges(ListArray::from_iter_primitive::<Float32Type, _, _>(
        (0..8).map(|i| {
            if i == 3 {
                None
            } else {
                Some(vec![Some(i as f32), Some(i as f32 + 0.25)])
            }
        }),
    ));
}

#[test]
fn float64_ranges_preserve_offsets_across_row_groups_and_nulls() {
    check_ranges(ListArray::from_iter_primitive::<Float64Type, _, _>(
        (0..8).map(|i| {
            if i == 3 {
                None
            } else {
                Some(vec![Some(i as f64), Some(i as f64 + 0.25)])
            }
        }),
    ));
}
