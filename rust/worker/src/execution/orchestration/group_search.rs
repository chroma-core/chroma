//! Group search orchestration for iterative group-by queries.
//!
//! This module provides the `GroupSearchCoordinator` which implements
//! Qdrant-style iterative fetching for grouped search results.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    spann_provider::SpannProvider,
    types::{materialize_logs, LogMaterializerError, MaterializeLogsResult},
};
use chroma_system::{ComponentHandle, Dispatcher, Orchestrator, System};
use chroma_types::{
    operator::{
        GroupBy, Knn, QueryVector, RecordMeasure, SearchPayloadResult, SearchRecord,
        SearchRecordGroup,
    },
    plan::SearchPayload,
    CollectionAndSegments, Metadata, SegmentType,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use thiserror::Error;
use tracing::{Instrument, Span};

use crate::execution::{
    operators::{
        fetch_log::FetchLogOperator,
        groups_aggregator::{GroupsAggregator, RecordGroup},
    },
    orchestration::{
        knn::KnnOrchestrator,
        knn_filter::{KnnFilterOrchestrator, KnnFilterOutput},
        spann_knn::SpannKnnOrchestrator,
        sparse_knn::SparseKnnOrchestrator,
    },
};

/// Constants for iterative group search
const MAX_GET_GROUPS_REQUESTS: usize = 5;
const MAX_GROUP_FILLING_REQUESTS: usize = 5;

#[derive(Error, Debug)]
pub enum GroupSearchError {
    #[error("KNN filter error: {0}")]
    KnnFilter(String),
    #[error("KNN search error: {0}")]
    Knn(String),
    #[error("Record segment error: {0}")]
    RecordSegment(String),
    #[error("Log materializer error: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Invalid group_by configuration")]
    InvalidGroupBy,
}

impl ChromaError for GroupSearchError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

/// Output from grouped search operation.
#[derive(Debug, Default)]
pub struct GroupSearchOutput {
    pub result: SearchPayloadResult,
    pub pulled_log_bytes: u64,
}

/// Coordinator for grouped search operations.
///
/// Implements iterative fetching to discover and fill groups:
/// - Phase 1: Discover groups (up to 5 iterations)
/// - Phase 2: Fill incomplete groups (up to 5 iterations)
pub struct GroupSearchCoordinator {
    // Providers
    blockfile_provider: BlockfileProvider,
    hnsw_provider: HnswIndexProvider,
    spann_provider: SpannProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    system: System,

    // Collection info
    collection_and_segments: CollectionAndSegments,

    // Search configuration
    search_payload: SearchPayload,
    group_by: GroupBy,

    // BM25 tenant list (for sparse search)
    bm25_tenant: Arc<HashSet<String>>,

    // State
    aggregator: GroupsAggregator,
    knn_filter_output: Option<KnnFilterOutput>,
    materialized_logs: Option<MaterializeLogsResult>,
}

impl GroupSearchCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        spann_provider: SpannProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        system: System,
        collection_and_segments: CollectionAndSegments,
        search_payload: SearchPayload,
        bm25_tenant: Arc<HashSet<String>>,
    ) -> Result<Self, GroupSearchError> {
        let group_by = search_payload
            .group_by
            .clone()
            .ok_or(GroupSearchError::InvalidGroupBy)?;

        let max_groups = search_payload.limit.limit.unwrap_or(10) as usize;
        let group_size = group_by.group_size as usize;

        let aggregator = GroupsAggregator::new(group_by.keys.clone(), max_groups, group_size);

        Ok(Self {
            blockfile_provider,
            hnsw_provider,
            spann_provider,
            dispatcher,
            system,
            collection_and_segments,
            search_payload,
            group_by,
            bm25_tenant,
            aggregator,
            knn_filter_output: None,
            materialized_logs: None,
        })
    }

    /// Runs the grouped search operation.
    pub async fn run(
        mut self,
        fetch_log: FetchLogOperator,
    ) -> Result<GroupSearchOutput, GroupSearchError> {
        // Handle uninitialized collection
        if self.collection_and_segments.is_uninitialized() {
            return Ok(GroupSearchOutput::default());
        }

        // Run KnnFilterOrchestrator once
        let knn_filter_orchestrator = KnnFilterOrchestrator::new(
            self.blockfile_provider.clone(),
            self.dispatcher.clone(),
            self.hnsw_provider.clone(),
            1000,
            self.collection_and_segments.clone(),
            fetch_log,
            self.search_payload.filter.clone(),
        );

        let knn_filter_output = knn_filter_orchestrator
            .run(self.system.clone())
            .await
            .map_err(|e| GroupSearchError::KnnFilter(e.to_string()))?;

        let pulled_log_bytes = knn_filter_output.fetch_log_bytes;

        // Create record segment reader
        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &self.collection_and_segments.record_segment,
            &self.blockfile_provider,
        ))
        .await
        {
            Ok(reader) => Some(reader),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => None,
            Err(e) => {
                return Err(GroupSearchError::RecordSegment(e.to_string()));
            }
        };

        // Materialize logs
        let materialized_logs = materialize_logs(
            &record_segment_reader,
            knn_filter_output.logs.clone(),
            None,
        )
        .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs for group search"))
        .await?;

        self.knn_filter_output = Some(knn_filter_output);
        self.materialized_logs = Some(materialized_logs);
        // We can't store reader with 'static lifetime easily, so we'll create it on demand

        // Phase 1: Discover groups
        for _ in 0..MAX_GET_GROUPS_REQUESTS {
            let new_records = self.run_knn_iteration().await?;

            if new_records.is_empty() {
                break;
            }

            // Fetch metadata and add to aggregator
            self.add_records_to_aggregator(&new_records).await?;

            if self.aggregator.has_enough_filled_groups() {
                break;
            }
        }

        // Phase 2: Fill incomplete groups (if needed)
        if !self.aggregator.has_enough_filled_groups() {
            for _ in 0..MAX_GROUP_FILLING_REQUESTS {
                let new_records = self.run_knn_iteration().await?;

                if new_records.is_empty() {
                    break;
                }

                self.add_records_to_aggregator(&new_records).await?;

                if self.aggregator.has_enough_filled_groups() {
                    break;
                }
            }
        }

        // Distill results and convert to output format
        let groups = self.aggregator.distill();
        let result = self.build_result(groups).await?;

        Ok(GroupSearchOutput {
            result,
            pulled_log_bytes,
        })
    }

    /// Runs a single KNN iteration and returns the results.
    async fn run_knn_iteration(&self) -> Result<Vec<RecordMeasure>, GroupSearchError> {
        let knn_filter_output = self
            .knn_filter_output
            .as_ref()
            .ok_or(GroupSearchError::KnnFilter("No filter output".to_string()))?;

        let knn_queries = self.search_payload.rank.knn_queries();
        if knn_queries.is_empty() {
            return Ok(Vec::new());
        }

        // Calculate fetch size: limit * group_size
        let max_groups = self.search_payload.limit.limit.unwrap_or(10);
        let group_size = self.group_by.group_size;
        let fetch = max_groups * group_size;

        let mut knn_futures = Vec::with_capacity(knn_queries.len());

        for knn_query in knn_queries {
            let knn_filter_output_clone = knn_filter_output.clone();
            let collection_and_segments_clone = self.collection_and_segments.clone();
            let system_clone = self.system.clone();
            let dispatcher = self.dispatcher.clone();
            let blockfile_provider = self.blockfile_provider.clone();
            let spann_provider = self.spann_provider.clone();
            let bm25_tenant = self.bm25_tenant.clone();

            knn_futures.push(async move {
                let result = match knn_query.query {
                    QueryVector::Dense(query) => {
                        let vector_segment_type =
                            collection_and_segments_clone.vector_segment.r#type;

                        if vector_segment_type == SegmentType::Spann {
                            let spann_orchestrator = SpannKnnOrchestrator::new(
                                spann_provider,
                                dispatcher,
                                1000,
                                collection_and_segments_clone,
                                knn_filter_output_clone,
                                fetch as usize,
                                query,
                            );

                            spann_orchestrator
                                .run(system_clone)
                                .await
                                .map_err(|e| GroupSearchError::Knn(e.to_string()))?
                        } else {
                            let knn = Knn {
                                embedding: query,
                                fetch,
                            };

                            let knn_orchestrator = KnnOrchestrator::new(
                                blockfile_provider,
                                dispatcher,
                                1000,
                                collection_and_segments_clone,
                                knn_filter_output_clone,
                                knn,
                            );

                            knn_orchestrator
                                .run(system_clone)
                                .await
                                .map_err(|e| GroupSearchError::Knn(e.to_string()))?
                        }
                    }
                    QueryVector::Sparse(query) => {
                        let tenant = collection_and_segments_clone.collection.tenant.clone();
                        let sparse_orchestrator = SparseKnnOrchestrator::new(
                            blockfile_provider,
                            dispatcher,
                            1000,
                            collection_and_segments_clone,
                            bm25_tenant.contains(&tenant),
                            knn_filter_output_clone,
                            query,
                            knn_query.key.to_string(),
                            fetch,
                        );

                        sparse_orchestrator
                            .run(system_clone)
                            .await
                            .map_err(|e| GroupSearchError::Knn(e.to_string()))?
                    }
                };

                Ok::<_, GroupSearchError>(result)
            });
        }

        let knn_results: Vec<Vec<RecordMeasure>> = stream::iter(knn_futures)
            .buffered(32)
            .try_collect()
            .await?;

        // Flatten results from all queries
        Ok(knn_results.into_iter().flatten().collect())
    }

    /// Fetches metadata for records and adds them to the aggregator.
    async fn add_records_to_aggregator(
        &mut self,
        records: &[RecordMeasure],
    ) -> Result<(), GroupSearchError> {
        if records.is_empty() {
            return Ok(());
        }

        // Build metadata map
        let metadata_map = self.fetch_metadata_for_records(records).await?;

        // Convert to aggregator format
        let record_tuples: Vec<_> = records
            .iter()
            .map(|r| (r.offset_id, r.measure))
            .collect();

        self.aggregator.add_points(&record_tuples, &metadata_map);

        Ok(())
    }

    /// Fetches metadata for the given records using materialized logs.
    async fn fetch_metadata_for_records(
        &self,
        records: &[RecordMeasure],
    ) -> Result<HashMap<u32, Metadata>, GroupSearchError> {
        let materialized_logs = self
            .materialized_logs
            .as_ref()
            .ok_or(GroupSearchError::KnnFilter("No materialized logs".to_string()))?;

        let offset_id_set: HashSet<u32> = records.iter().map(|r| r.offset_id).collect();

        // Create a mapping from offset_id to materialized log entry
        let offset_id_to_log: HashMap<u32, _> = materialized_logs
            .iter()
            .filter(|log| offset_id_set.contains(&log.get_offset_id()))
            .map(|log| (log.get_offset_id(), log))
            .collect();

        let mut metadata_map = HashMap::new();

        // Create record segment reader for this operation
        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &self.collection_and_segments.record_segment,
            &self.blockfile_provider,
        ))
        .await
        {
            Ok(reader) => Some(reader),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => None,
            Err(e) => {
                return Err(GroupSearchError::RecordSegment(e.to_string()));
            }
        };

        for record in records {
            if let Some(log) = offset_id_to_log.get(&record.offset_id) {
                // Get metadata from materialized log
                let hydrated = log
                    .hydrate(record_segment_reader.as_ref())
                    .await
                    .map_err(GroupSearchError::LogMaterializer)?;
                let meta = hydrated.merged_metadata();
                if !meta.is_empty() {
                    metadata_map.insert(record.offset_id, meta);
                }
            } else if let Some(reader) = &record_segment_reader {
                // Get from segment
                if let Ok(Some(data_record)) = reader.get_data_for_offset_id(record.offset_id).await
                {
                    if let Some(meta) = data_record.metadata {
                        metadata_map.insert(record.offset_id, meta);
                    }
                }
            }
        }

        Ok(metadata_map)
    }

    /// Builds the final SearchPayloadResult from distilled groups.
    async fn build_result(
        &self,
        groups: Vec<RecordGroup>,
    ) -> Result<SearchPayloadResult, GroupSearchError> {
        let materialized_logs = self
            .materialized_logs
            .as_ref()
            .ok_or(GroupSearchError::KnnFilter("No materialized logs".to_string()))?;

        // Create record segment reader for this operation
        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &self.collection_and_segments.record_segment,
            &self.blockfile_provider,
        ))
        .await
        {
            Ok(reader) => Some(reader),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => None,
            Err(e) => {
                return Err(GroupSearchError::RecordSegment(e.to_string()));
            }
        };

        // Collect all offset_ids we need
        let all_offset_ids: HashSet<u32> = groups
            .iter()
            .flat_map(|g| g.records.iter().map(|r| r.offset_id))
            .collect();

        // Build mapping from offset_id to materialized log
        let offset_id_to_log: HashMap<u32, _> = materialized_logs
            .iter()
            .filter(|log| all_offset_ids.contains(&log.get_offset_id()))
            .map(|log| (log.get_offset_id(), log))
            .collect();

        let mut result_groups = Vec::with_capacity(groups.len());

        for group in groups {
            let mut search_records = Vec::with_capacity(group.records.len());

            for record in &group.records {
                let (user_id, document, metadata) = if let Some(log) =
                    offset_id_to_log.get(&record.offset_id)
                {
                    let hydrated = log
                        .hydrate(record_segment_reader.as_ref())
                        .await
                        .map_err(GroupSearchError::LogMaterializer)?;
                    (
                        hydrated.get_user_id().to_string(),
                        hydrated.merged_document_ref().map(|s| s.to_string()),
                        {
                            let meta = hydrated.merged_metadata();
                            if meta.is_empty() {
                                None
                            } else {
                                Some(meta)
                            }
                        },
                    )
                } else if let Some(reader) = &record_segment_reader {
                    if let Ok(Some(data_record)) =
                        reader.get_data_for_offset_id(record.offset_id).await
                    {
                        (
                            data_record.id.to_string(),
                            data_record.document.map(|s| s.to_string()),
                            data_record.metadata,
                        )
                    } else {
                        (format!("offset_{}", record.offset_id), None, None)
                    }
                } else {
                    (format!("offset_{}", record.offset_id), None, None)
                };

                search_records.push(SearchRecord {
                    id: user_id,
                    document,
                    embedding: None, // Don't include embeddings in grouped results by default
                    metadata,
                    score: Some(record.score),
                });
            }

            result_groups.push(SearchRecordGroup {
                group_key_values: group.group_key.to_string_values(),
                records: search_records,
            });
        }

        Ok(SearchPayloadResult {
            records: Vec::new(), // Empty for grouped results
            groups: result_groups,
        })
    }
}
