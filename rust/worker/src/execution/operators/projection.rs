use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_record::{
        RecordSegmentReaderOptions, RecordSegmentReaderShard, RecordSegmentReaderShardCreationError,
    },
    bloom_filter::BloomFilterManager,
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::{Projection, ProjectionOutput, ProjectionRecord},
    Chunk, LogRecord, Segment, SegmentShard,
};
use futures::future::try_join_all;
use thiserror::Error;
use tracing::{Instrument, Span};

/// The `Projection` operator retrieves record content by offset ids
///
/// # Inputs
/// - `logs`: The latest logs of the collection
/// - `blockfile_provider`: The blockfile provider
/// - `record_segment`: The record segment information
/// - `offset_ids`: The offset ids in either logs or blockfile to retrieve for
///
/// # Outputs
/// - `records`: The retrieved records in the same order as `offset_ids`
///
/// # Usage
/// It can be used to retrieve record contents as user requested
/// It should be run as the last step of an orchestrator
#[derive(Clone, Debug)]
pub struct ProjectionInput {
    pub logs: Chunk<LogRecord>,
    pub blockfile_provider: BlockfileProvider,
    pub record_segment: Segment,
    pub offset_ids: Vec<u32>,
    pub bloom_filter_manager: Option<BloomFilterManager>,
}

#[derive(Error, Debug)]
pub enum ProjectionError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderShardCreationError),
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
    #[error("Error reading unitialized record segment")]
    RecordSegmentUninitialized,
    #[error("Error reading phantom record: {0}")]
    RecordSegmentPhantomRecord(u32),
}

impl ChromaError for ProjectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ProjectionError::LogMaterializer(e) => e.code(),
            ProjectionError::RecordReader(e) => e.code(),
            ProjectionError::RecordSegment(e) => e.code(),
            ProjectionError::RecordSegmentUninitialized => ErrorCodes::Internal,
            ProjectionError::RecordSegmentPhantomRecord(_) => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<ProjectionInput, ProjectionOutput> for Projection {
    type Error = ProjectionError;

    async fn run(&self, input: &ProjectionInput) -> Result<ProjectionOutput, ProjectionError> {
        if input.offset_ids.is_empty() {
            return Ok(ProjectionOutput { records: vec![] });
        }

        let needs_data = self.document || self.embedding || self.metadata;

        tracing::trace!(
            "Running projection on {} offset ids (needs_data={})",
            input.offset_ids.len(),
            needs_data,
        );
        let record_segment_shard = SegmentShard::from((&input.record_segment, 0));
        let record_segment_reader = match Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &input.blockfile_provider,
            input.bloom_filter_manager.clone(),
        ))
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            Err(e)
                if matches!(
                    *e,
                    RecordSegmentReaderShardCreationError::UninitializedSegment
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;

        let offset_id_set: HashSet<_> = HashSet::from_iter(input.offset_ids.iter().cloned());

        // Prefetch: when needs_data, load full data records (which contain the user ID);
        // otherwise, load only the lightweight id_to_user_id mapping.
        if let Some(reader) = &record_segment_reader {
            if needs_data {
                reader
                    .load_id_to_data(offset_id_set.iter().cloned())
                    .instrument(tracing::trace_span!(parent: Span::current(), "Load ID to data", num_ids = offset_id_set.len()))
                    .await;
            } else {
                reader
                    .load_id_to_user_id(offset_id_set.iter().cloned())
                    .instrument(tracing::trace_span!(parent: Span::current(), "Load ID to user ID", num_ids = offset_id_set.len()))
                    .await;
            }
        }

        let plan = RecordSegmentReaderOptions {
            use_bloom_filter: input
                .bloom_filter_manager
                .as_ref()
                .is_some_and(|mgr| input.logs.len() >= mgr.storage_fetch_threshold()),
        };
        let materialized_logs =
            materialize_logs(&record_segment_reader, input.logs.clone(), None, &plan)
                .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
                .await?;

        // Create a hash map that maps an offset id to the corresponding log
        // It contains all records from the logs that should be present in the final result
        let offset_id_to_log_record: HashMap<_, _> = materialized_logs
            .iter()
            .flat_map(|log| {
                offset_id_set
                    .contains(&log.get_offset_id())
                    .then_some((log.get_offset_id(), log))
            })
            .collect();

        let records: Vec<ProjectionRecord> = if needs_data {
            // Full hydration: use concurrent futures (query result sets are typically bounded)
            let current_span = Span::current();
            let futures: Vec<_> = input
                .offset_ids
                .iter()
                .map(|offset_id| {
                    async {
                        let (id, document, embedding, metadata) =
                            match offset_id_to_log_record.get(offset_id) {
                                Some(log) => {
                                    let log = log
                                        .hydrate(record_segment_reader.as_ref())
                                        .await
                                        .map_err(ProjectionError::LogMaterializer)?;
                                    (
                                        log.get_user_id().to_string(),
                                        log.merged_document_ref()
                                            .filter(|_| self.document)
                                            .map(str::to_string),
                                        self.embedding
                                            .then_some(log.merged_embeddings_ref().to_vec()),
                                        self.metadata
                                            .then_some(log.merged_metadata())
                                            .filter(|metadata| !metadata.is_empty()),
                                    )
                                }
                                None => {
                                    let reader = record_segment_reader
                                        .as_ref()
                                        .ok_or(ProjectionError::RecordSegmentUninitialized)?;
                                    let record =
                                        reader.get_data_for_offset_id(*offset_id).await?.ok_or(
                                            ProjectionError::RecordSegmentPhantomRecord(*offset_id),
                                        )?;
                                    (
                                        record.id.to_string(),
                                        record
                                            .document
                                            .filter(|_| self.document)
                                            .map(str::to_string),
                                        self.embedding.then_some(record.embedding.to_vec()),
                                        record.metadata.filter(|_| self.metadata),
                                    )
                                }
                            };

                        Ok::<_, ProjectionError>(ProjectionRecord {
                            id,
                            document,
                            embedding,
                            metadata,
                        })
                    }
                    .instrument(current_span.clone())
                })
                .collect();
            try_join_all(futures).await?
        } else {
            // Lightweight ID-only path (e.g. delete-where): iterate sequentially
            // instead of spawning a future per offset ID. Blocks are already
            // prefetched above, so each get_user_id_for_offset_id hits the cache
            // and resolves without I/O.
            let mut records = Vec::with_capacity(input.offset_ids.len());
            for offset_id in &input.offset_ids {
                let id = match offset_id_to_log_record.get(offset_id) {
                    Some(log) => log
                        .get_user_id(record_segment_reader.as_ref())
                        .await
                        .map_err(ProjectionError::LogMaterializer)?,
                    None => match &record_segment_reader {
                        Some(reader) => reader
                            .get_user_id_for_offset_id(*offset_id)
                            .await?
                            .to_string(),
                        None => return Err(ProjectionError::RecordSegmentUninitialized),
                    },
                };
                records.push(ProjectionRecord {
                    id,
                    document: None,
                    embedding: None,
                    metadata: None,
                });
            }
            records
        };

        Ok(ProjectionOutput { records })
    }
}

#[cfg(test)]
mod tests {
    use chroma_log::test::{int_as_id, upsert_generator, LoadFromGenerator, LogGenerator};
    use chroma_segment::test::TestDistributedSegment;
    use chroma_system::Operator;
    use chroma_types::operator::Projection;

    use super::ProjectionInput;

    /// The unit tests for `Projection` operator uses the following test data
    /// It first generates 100 log records and compact them,
    /// then generate 20 log records that overwrite the compacted data,
    /// and finally generate 20 log records of new data:
    ///
    /// - Log: Upsert [81..=120]
    /// - Compacted: Upsert [1..=100]
    async fn setup_projection_input(
        offset_ids: Vec<u32>,
    ) -> (TestDistributedSegment, ProjectionInput) {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(100, upsert_generator)
            .await;
        let blockfile_provider = test_segment.blockfile_provider.clone();
        let record_segment = test_segment.record_segment.clone();
        (
            test_segment,
            ProjectionInput {
                logs: upsert_generator.generate_chunk(81..=120),
                blockfile_provider,
                record_segment,
                offset_ids,
                bloom_filter_manager: None,
            },
        )
    }

    #[tokio::test]
    async fn test_trivial_projection() {
        let (_test_segment, projection_input) = setup_projection_input((1..=120).collect()).await;

        let projection_operator = Projection {
            document: false,
            embedding: false,
            metadata: false,
        };

        let projection_output = projection_operator
            .run(&projection_input)
            .await
            .expect("ProjectionOperator should not fail");

        assert_eq!(projection_output.records.len(), 120);
        for (offset, record) in projection_output.records.into_iter().enumerate() {
            assert_eq!(record.id, int_as_id(offset + 1));
            assert!(record.document.is_none());
            assert!(record.embedding.is_none());
            assert!(record.metadata.is_none());
        }
    }

    #[tokio::test]
    async fn test_full_projection() {
        let (_test_segment, projection_input) = setup_projection_input((1..=120).collect()).await;

        let projection_operator = Projection {
            document: true,
            embedding: true,
            metadata: true,
        };

        let projection_output = projection_operator
            .run(&projection_input)
            .await
            .expect("ProjectionOperator should not fail");

        assert_eq!(projection_output.records.len(), 120);
        for (offset, record) in projection_output.records.into_iter().enumerate() {
            assert_eq!(record.id, int_as_id(offset + 1));
            assert!(record.document.is_some());
            assert!(record.embedding.is_some());
            assert!(record.metadata.is_some());
        }
    }
}
