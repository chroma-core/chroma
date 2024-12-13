use chroma_benchmark::datasets::sift::Sift1MData;
use chroma_types::{Chunk, LogRecord, Operation, OperationRecord};
use indicatif::ProgressIterator;
use worker::{log::test::modulo_metadata, segment::test::TestSegment};

const DATA_CHUNK_SIZE: usize = 10000;

pub async fn sift1m_segments() -> TestSegment {
    let mut segments = TestSegment::default();
    let mut sift1m = Sift1MData::init()
        .await
        .expect("Should be able to download Sift1M data");

    for chunk_start in (0..Sift1MData::collection_size())
        .step_by(DATA_CHUNK_SIZE)
        .progress()
        .with_message("Loading Sift1M Data")
    {
        let embedding_chunk = sift1m
            .data_range(chunk_start..(chunk_start + DATA_CHUNK_SIZE))
            .await
            .expect("Should be able to decode data chunk");

        let log_records = embedding_chunk
            .into_iter()
            .enumerate()
            .map(|(index, embedding)| LogRecord {
                log_offset: (chunk_start + index) as i64,
                record: OperationRecord {
                    id: (chunk_start + index).to_string(),
                    embedding: Some(embedding),
                    encoding: None,
                    metadata: Some(modulo_metadata(chunk_start + index)),
                    document: None,
                    operation: Operation::Add,
                },
            })
            .collect::<Vec<_>>();
        segments
            .compact_log(Chunk::new(log_records.into()), chunk_start)
            .await;
    }
    segments
}
