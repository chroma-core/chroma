//! Parallel decoding must retain source row IDs and exact float values.
#[path = "../benches/datasets/parquet_range.rs"]
mod parquet_range;
#[path = "../benches/datasets/wikipedia_shards.rs"]
mod shards;

use arrow::array::{Array, ListArray};
use arrow::datatypes::{Field, Float32Type, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::sync::Arc;

fn fixture(embeddings: ListArray) -> tempfile::NamedTempFile {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "emb",
        embeddings.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(embeddings)]).unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(2)
        .build();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

#[test]
fn parallel_shards_preserve_order_null_ids_and_float_conversion() {
    let f32_file = fixture(ListArray::from_iter_primitive::<Float32Type, _, _>([
        Some(vec![Some(0.0), Some(-0.0)]),
        None,
        Some(vec![Some(2.0), Some(2.25)]),
        Some(vec![Some(3.0), Some(3.25)]),
    ]));
    let f64_file = fixture(ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(4.0), Some(4.25)]),
        Some(vec![Some(5.0), Some(5.25)]),
        None,
        Some(vec![Some(7.0), Some(7.25)]),
    ]));
    let ranges = vec![
        shards::ShardRange {
            path: f32_file.path().into(),
            first_id: 0,
            offset: 0,
            len: 4,
        },
        shards::ShardRange {
            path: f64_file.path().into(),
            first_id: 4,
            offset: 0,
            len: 4,
        },
        shards::ShardRange {
            path: f32_file.path().into(),
            first_id: 10,
            offset: 2,
            len: 2,
        },
        shards::ShardRange {
            path: f64_file.path().into(),
            first_id: 12,
            offset: 0,
            len: 2,
        },
        shards::ShardRange {
            path: f32_file.path().into(),
            first_id: 16,
            offset: 0,
            len: 1,
        },
    ];
    let serial = shards::load_ranges(&ranges, 1).unwrap();
    assert_eq!(
        serial.iter().map(|v| v.0).collect::<Vec<_>>(),
        [0, 2, 3, 4, 5, 7, 10, 11, 12, 13, 16]
    );
    assert_eq!(serial[0].1[1].to_bits(), (-0.0f32).to_bits());
    assert_eq!(&*serial[3].1, &[4.0, 4.25]);
    for workers in [0, 2, 4, 32] {
        let parallel = shards::load_ranges(&ranges, workers).unwrap();
        for ((expected_id, expected), (actual_id, actual)) in serial.iter().zip(&parallel) {
            assert_eq!(expected_id, actual_id);
            assert_eq!(
                expected.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                actual.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
            );
        }
        assert_eq!(parallel.len(), serial.len());
    }
    assert!(shards::load_ranges(&[], 4).unwrap().is_empty());
}

#[test]
fn decoder_failure_returns_an_error_instead_of_partial_vectors() {
    let missing = shards::ShardRange {
        path: "/nonexistent/wikipedia-test-shard".into(),
        first_id: 0,
        offset: 0,
        len: 1,
    };
    let file = tempfile::NamedTempFile::new().unwrap();
    let invalid = shards::ShardRange {
        path: file.path().into(),
        first_id: 1,
        offset: 0,
        len: 1,
    };
    assert!(shards::load_ranges(&[missing, invalid], 4).is_err());
}
