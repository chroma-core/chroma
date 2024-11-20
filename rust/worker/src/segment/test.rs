use std::sync::atomic::AtomicU32;

use chroma_blockstore::{provider::BlockfileProvider, test_arrow_blockfile_provider};
use chroma_index::{hnsw_provider::HnswIndexProvider, test_hnsw_index_provider};
use chroma_types::{
    test_segment, Chunk, Collection, CollectionUuid, LogRecord, OperationRecord, Segment,
    SegmentScope,
};

use crate::log::test::{LogGenerator, TEST_EMBEDDING_DIMENSION};

use super::{
    materialize_logs, metadata_segment::MetadataSegmentWriter, record_segment::RecordSegmentWriter,
    SegmentFlusher, SegmentWriter,
};

pub struct TestSegment {
    pub hnsw_provider: HnswIndexProvider,
    pub blockfile_provider: BlockfileProvider,
    pub collection: Collection,
    pub metadata_segment: Segment,
    pub record_segment: Segment,
    pub vector_segment: Segment,
}

impl TestSegment {
    // WARN: The size of the log chunk should not be too large
    async fn compact_log(&mut self, logs: Chunk<LogRecord>, offset: usize) {
        let materialized_logs =
            materialize_logs(&None, &logs, Some(AtomicU32::new(offset as u32).into()))
                .await
                .expect("Should be able to materialize log.");

        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&self.metadata_segment, &self.blockfile_provider)
                .await
                .expect("Should be able to initialize metadata writer.");
        metadata_writer
            .apply_materialized_log_chunk(materialized_logs.clone())
            .await
            .expect("Should be able to apply materialized logs.");
        metadata_writer
            .write_to_blockfiles()
            .await
            .expect("Should be able to write to blockfile.");
        self.metadata_segment.file_path = metadata_writer
            .commit()
            .await
            .expect("Should be able to commit metadata.")
            .flush()
            .await
            .expect("Should be able to flush metadata.");

        let record_writer =
            RecordSegmentWriter::from_segment(&self.record_segment, &self.blockfile_provider)
                .await
                .expect("Should be able to initiaize record writer.");
        record_writer
            .apply_materialized_log_chunk(materialized_logs)
            .await
            .expect("Should be able to apply materialized log.");

        self.record_segment.file_path = record_writer
            .commit()
            .await
            .expect("Should be able to commit metadata.")
            .flush()
            .await
            .expect("Should be able to flush metadata.");
    }

    pub async fn populate_with_generator<G>(&mut self, size: usize, generator: &LogGenerator<G>)
    where
        G: Fn(usize) -> OperationRecord,
    {
        let ids: Vec<_> = (1..=size).collect();
        for chunk in ids.chunks(100) {
            self.compact_log(
                generator.generate_chunk(chunk.iter().copied()),
                chunk
                    .first()
                    .copied()
                    .expect("The chunk of offset ids to generate should not be empty.")
                    - 1,
            )
            .await;
        }
    }
}

impl Default for TestSegment {
    fn default() -> Self {
        let collection_uuid = CollectionUuid::new();
        let collection = Collection {
            collection_id: collection_uuid,
            name: "Test Collection".to_string(),
            metadata: None,
            dimension: Some(TEST_EMBEDDING_DIMENSION as i32),
            tenant: "Test Tenant".to_string(),
            database: String::new(),
            log_position: 0,
            version: 0,
        };
        Self {
            hnsw_provider: test_hnsw_index_provider(),
            blockfile_provider: test_arrow_blockfile_provider(2 << 22),
            collection,
            metadata_segment: test_segment(collection_uuid, SegmentScope::METADATA),
            record_segment: test_segment(collection_uuid, SegmentScope::RECORD),
            vector_segment: test_segment(collection_uuid, SegmentScope::VECTOR),
        }
    }
}
