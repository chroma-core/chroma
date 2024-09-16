use std::{collections::HashMap, str::FromStr};

use chroma_benchmark_datasets::{datasets::scidocs::SciDocsDataset, types::BenchmarkDataset};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::{
    cache::Cache,
    config::{CacheConfig, UnboundedCacheConfig},
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{Chunk, LogRecord};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use uuid::Uuid;
use worker::segment::{
    metadata_segment::{MetadataSegmentReader, MetadataSegmentWriter},
    types::{LogMaterializer, SegmentWriter},
};

async fn get_log_chunk() -> Chunk<LogRecord> {
    // todo: Result?
    let dataset = SciDocsDataset::init()
        .await
        .expect("Failed to initialize SciDocs dataset");

    let log_records = dataset
        .create_log_stream(|_| async { vec![0.0, 0.0, 0.0] })
        .await
        .expect("Failed to create log stream")
        .filter_map(|record| async {
            match record {
                Ok(record) => Some(record),
                Err(_) => None,
            }
        })
        .collect::<Vec<_>>()
        .await;

    Chunk::new(log_records[..1000].to_vec().into())
}

async fn get_reader(chunk: Chunk<LogRecord>) -> MetadataSegmentReader<'static> {
    let materializer = LogMaterializer::new(None, chunk, None);

    let metadata_segment = chroma_types::Segment {
        id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
        r#type: chroma_types::SegmentType::BlockfileMetadata,
        scope: chroma_types::SegmentScope::METADATA,
        collection: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
        metadata: None,
        file_path: HashMap::new(),
    };

    const BLOCK_SIZE: usize = 8 * 1024 * 1024; // 8MB

    let tmp_dir = tempfile::tempdir().unwrap();
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
    let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
    let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
    let arrow_blockfile_provider =
        ArrowBlockfileProvider::new(storage, BLOCK_SIZE, block_cache, sparse_index_cache);
    let blockfile_provider = BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

    let metadata_writer =
        MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
            .await
            .expect("Error creating segment writer");

    metadata_writer
        .apply_materialized_log_chunk(materializer.materialize().await.unwrap())
        .await
        .unwrap();

    MetadataSegmentReader::from_segment(&metadata_segment, &blockfile_provider)
        .await
        .expect("Metadata segment reader construction failed")
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let runner = tokio::runtime::Builder::new_multi_thread()
        .build()
        .expect("Failed to create runtime");

    let log_chunk = runner.block_on(get_log_chunk());

    c.bench_function("fib 20", |b| {
        b.to_async(&runner).iter_batched(
            || log_chunk.clone(),
            |chunk| async move {
                let reader = get_reader(black_box(chunk)).await;
            },
            // todo: correct size?
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
