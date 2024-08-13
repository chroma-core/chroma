use arrow::ipc::Block;
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::{
    cache::Cache,
    config::{CacheConfig, UnboundedCacheConfig},
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::{Chunk, LogRecord, OperationRecord, ScalarEncoding};
use std::{collections::HashMap, io::Read, str::FromStr};
use uuid::Uuid;
// TODO: move into own module so we don't have this ugly import
use worker::execution::operator::Operator;
use worker::{
    execution::operators::partition::PartitionOperator,
    segment::{
        metadata_segment::MetadataSegmentWriter,
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        types::{LogMaterializer, SegmentWriter},
    },
};

// TODO: move dataset into a separate module that is shareable with good manipulation
enum BenchmarkDataset {
    SCIDOCS,
}

impl BenchmarkDataset {
    fn get_url(&self) -> &str {
        match self {
            BenchmarkDataset::SCIDOCS => {
                "https://huggingface.co/datasets/BeIR/scidocs/resolve/main/corpus.jsonl.gz"
            }
        }
    }
}

// Simple FTS focused dataset
struct Dataset {
    documents: Vec<String>,
}

fn download_dataset(dataset: BenchmarkDataset) -> Result<Dataset, Box<dyn std::error::Error>> {
    let url = dataset.get_url();
    // TODO: cache the dataset
    let download = reqwest::blocking::get(url)?;
    let as_bytes = download.bytes()?;
    let mut decoder = flate2::read::GzDecoder::new(as_bytes.as_ref());
    let mut as_string = String::new();
    decoder.read_to_string(&mut as_string)?;

    let deser = serde_json::Deserializer::from_str(&as_string).into_iter::<serde_json::Value>();
    let mut ret_docs = Vec::new();
    for row in deser {
        let row = row?;
        let text = row["text"].as_str().unwrap();
        ret_docs.push(text.to_string());
    }

    Ok(Dataset {
        documents: ret_docs,
    })
}

async fn get_writer(blockfile_provider: BlockfileProvider) -> MetadataSegmentWriter<'static> {
    let metadata_segment = chroma_types::Segment {
        id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
        r#type: chroma_types::SegmentType::BlockfileMetadata,
        scope: chroma_types::SegmentScope::METADATA,
        collection: Some(
            Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
        ),
        metadata: None,
        file_path: HashMap::new(),
    };

    let metadata_writer =
        MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
            .await
            .expect("Error creating segment writer");

    metadata_writer
}

async fn get_materializer(
    blockfile_provider: BlockfileProvider,
    data: Chunk<LogRecord>,
) -> LogMaterializer<'static> {
    let materializer = LogMaterializer::new(None, data, None);
    materializer
}

fn dataset_to_log(dataset: Dataset) -> Vec<LogRecord> {
    let mut ret_records = Vec::new();
    let mut curr_offset = 0;
    for doc in dataset.documents {
        let record = LogRecord {
            log_offset: curr_offset,
            record: OperationRecord {
                id: curr_offset.to_string(),
                embedding: Some(vec![0.0, 0.0]),
                encoding: Some(ScalarEncoding::FLOAT32),
                metadata: None,
                document: Some(doc),
                operation: chroma_types::Operation::Add,
            },
        };

        curr_offset += 1;
        ret_records.push(record);
    }
    return ret_records;
}

#[tokio::main(worker_threads = 4)]
async fn main() {
    let dataset = download_dataset(BenchmarkDataset::SCIDOCS).unwrap();
    println!(
        "Downloaded dataset with {} documents",
        dataset.documents.len()
    );
    const BLOCK_SIZE: usize = 8 * 1024 * 1024; // 8MB

    let tmp_dir = tempfile::tempdir().unwrap();
    let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
    let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
    let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
    let arrow_blockfile_provider =
        ArrowBlockfileProvider::new(storage, BLOCK_SIZE, block_cache, sparse_index_cache);
    let blockfile_provider = BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

    let writer = get_writer(blockfile_provider.clone()).await;
    let log_records = dataset_to_log(dataset);
    let materializer =
        get_materializer(blockfile_provider.clone(), Chunk::new(log_records.into())).await;
    let start_time = std::time::Instant::now();
    writer
        .apply_materialized_log_chunk(materializer.materialize().await.unwrap())
        .await
        .unwrap();
    println!("Took {:?}", start_time.elapsed());

    // Partition approach
    // let partition_operator = PartitionOperator::new();
    // let partition_operator_input = worker::execution::operators::partition::PartitionInput {
    //     records: Chunk::new(dataset_to_log(dataset).into()),
    //     max_partition_size: 100,
    // };
    // let partition_result = partition_operator
    //     .run(&partition_operator_input)
    //     .await
    //     .unwrap();

    // let record_chunks = partition_result.records;

    // // create a new task for each chunk
    // let mut tasks = Vec::new();
    // let start_time = std::time::Instant::now();
    // for chunk in record_chunks {
    //     let blockfile_provider = blockfile_provider.clone();
    //     let writer_clone = writer.clone();
    //     let task = tokio::spawn(async move {
    //         let materializer = get_materializer(blockfile_provider.clone(), chunk).await;

    //         let materialized = materializer
    //             .materialize()
    //             .await
    //             .expect("Error materializing");

    //         writer_clone
    //             .apply_materialized_log_chunk(materialized)
    //             .await
    //             .unwrap();
    //     });
    //     tasks.push(task);
    // }

    // for task in tasks {
    //     task.await.unwrap();
    // }
    // println!("Took {:?}", start_time.elapsed());
}
