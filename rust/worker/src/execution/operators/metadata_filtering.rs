use std::{
    collections::{BTreeMap, HashMap},
    ops::{BitAnd, BitOr, Bound},
};

use crate::{
    execution::operator::Operator,
    segment::{
        metadata_segment::{MetadataSegmentError, MetadataSegmentReader},
        record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
        LogMaterializer, LogMaterializerError, MaterializedLogRecord,
    },
};
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::metadata::types::MetadataIndexError;
use chroma_types::{
    BooleanOperator, Chunk, DirectDocumentComparison, DirectWhereComparison, DocumentOperator,
    LogRecord, MaterializedLogOperation, MetadataSetValue, MetadataValue, PrimitiveOperator,
    Segment, SetOperator, SignedRoaringBitmap, Where, WhereChildren, WhereComparison,
};
use roaring::RoaringBitmap;
use thiserror::Error;
use tonic::async_trait;
use tracing::{trace, Instrument, Span};

/// # Description
/// The `MetadataFilteringOperator` should produce the offset ids of the matching documents.
///
/// # Input
/// - `blockfile_provider` / `record_segment` / `metadata_segment`: handles to the underlying data.
/// - `log_record`: the chunk of log that is not yet compacted, representing the latest updates.
/// - `query_ids`: user provided ids, which must be a superset of returned documents.
/// - `where_clause`: a boolean predicate on the metadata and the content of the document.
/// - `offset`: the number of records with smallest offset ids to skip, if specified
/// - `limit`: the number of records with smallest offset ids to take after the skip, if specified
///
/// # Output
/// - `log_record`: the same `log_record` from the input.
/// - `offset_ids`: the matching offset ids (in both log and compact storage).
///
/// # Note
/// - The `MetadataProvider` enum can be viewed as an universal interface for the metadata and document index.
/// - In the output, `log_mask` should be a subset of `offset_ids`

#[derive(Debug)]
pub struct MetadataFilteringOperator {}

impl MetadataFilteringOperator {
    pub fn new() -> Box<Self> {
        Box::new(MetadataFilteringOperator {})
    }
}

#[derive(Debug)]
pub struct MetadataFilteringInput {
    blockfile_provider: BlockfileProvider,
    record_segment: Segment,
    metadata_segment: Segment,
    log_record: Chunk<LogRecord>,
    query_ids: Option<Vec<String>>,
    where_clause: Option<Where>,
    offset: Option<u32>,
    limit: Option<u32>,
}

impl MetadataFilteringInput {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        record_segment: Segment,
        metadata_segment: Segment,
        log_record: Chunk<LogRecord>,
        query_ids: Option<Vec<String>>,
        where_clause: Option<Where>,
        offset: Option<u32>,
        limit: Option<u32>,
    ) -> Self {
        Self {
            blockfile_provider,
            record_segment,
            metadata_segment,
            log_record,
            query_ids,
            where_clause,
            offset,
            limit,
        }
    }
}

#[derive(Debug)]
pub struct MetadataFilteringOutput {
    pub log_records: Chunk<LogRecord>,
    pub offset_ids: RoaringBitmap,
}

#[derive(Error, Debug)]
pub enum MetadataFilteringError {
    #[error("Error creating record segment reader {0}")]
    RecordSegmentReaderCreationError(#[from] RecordSegmentReaderCreationError),
    #[error("Error materializing logs {0}")]
    LogMaterializationError(#[from] LogMaterializerError),
    #[error("Error filtering documents by where or where_document clauses {0}")]
    IndexError(#[from] MetadataIndexError),
    #[error("Error from metadata segment reader {0}")]
    MetadataSegmentReaderError(#[from] MetadataSegmentError),
    #[error("Error reading from record segment")]
    RecordSegmentReaderError,
    #[error("Invalid input")]
    InvalidInput,
}

impl ChromaError for MetadataFilteringError {
    fn code(&self) -> ErrorCodes {
        match self {
            MetadataFilteringError::RecordSegmentReaderCreationError(e) => e.code(),
            MetadataFilteringError::LogMaterializationError(e) => e.code(),
            MetadataFilteringError::IndexError(e) => e.code(),
            MetadataFilteringError::MetadataSegmentReaderError(e) => e.code(),
            MetadataFilteringError::RecordSegmentReaderError => ErrorCodes::Internal,
            MetadataFilteringError::InvalidInput => ErrorCodes::InvalidArgument,
        }
    }
}

/// This sturct provides an abstraction over the materialized logs that is similar to the metadata segment
pub(crate) struct MetadataLogReader<'me> {
    // This maps metadata keys to a `BTreeMap` mapping values to offset ids
    // This mimics the layout in the metadata segment
    // //TODO: Maybe a sorted vector with binary search is more lightweight and performant?
    compact_metadata: HashMap<&'me str, BTreeMap<&'me MetadataValue, RoaringBitmap>>,
    // This maps offset ids to documents, excluding deleted ones
    document: HashMap<u32, &'me str>,
    // This contains all offset ids that is present in the materialized log
    domain: RoaringBitmap,
    // This maps user ids to offset ids, excluding deleted ones
    // The value set should be a subset of `domain`
    uid_to_oid: HashMap<&'me str, u32>,
}

impl<'me> MetadataLogReader<'me> {
    pub(crate) fn new(logs: &'me Chunk<MaterializedLogRecord<'me>>) -> Self {
        let mut compact_metadata: HashMap<_, BTreeMap<&MetadataValue, RoaringBitmap>> =
            HashMap::new();
        let mut document = HashMap::new();
        let mut domain = RoaringBitmap::new();
        let mut uid_to_oid = HashMap::new();
        for (log, _) in logs.iter() {
            domain.insert(log.offset_id);
            if !matches!(
                log.final_operation,
                MaterializedLogOperation::DeleteExisting
            ) {
                uid_to_oid.insert(log.merged_user_id_ref(), log.offset_id);
                let log_meta = log.merged_metadata_ref();
                for (key, val) in log_meta.into_iter() {
                    compact_metadata
                        .entry(key)
                        .or_default()
                        .entry(val)
                        .or_default()
                        .insert(log.offset_id);
                }
                if let Some(doc) = log.merged_document_ref() {
                    document.insert(log.offset_id, doc);
                }
            }
        }
        Self {
            compact_metadata,
            document,
            domain,
            uid_to_oid,
        }
    }
    pub(crate) fn get(
        &self,
        key: &str,
        val: &MetadataValue,
        op: &PrimitiveOperator,
    ) -> Result<RoaringBitmap, MetadataFilteringError> {
        use Bound::*;
        use PrimitiveOperator::*;
        if let Some(btm) = self.compact_metadata.get(key) {
            let bounds = match op {
                Equal => (Included(&val), Included(&val)),
                GreaterThan => (Excluded(&val), Unbounded),
                GreaterThanOrEqual => (Included(&val), Unbounded),
                LessThan => (Unbounded, Excluded(&val)),
                LessThanOrEqual => (Unbounded, Included(&val)),
                // Inequality filter is not supported at metadata provider level
                NotEqual => return Err(MetadataFilteringError::InvalidInput),
            };
            Ok(btm
                .range::<&MetadataValue, _>(bounds)
                .map(|(_, v)| v)
                .fold(RoaringBitmap::new(), BitOr::bitor))
        } else {
            Ok(RoaringBitmap::new())
        }
    }

    pub(crate) fn search_user_ids(&self, uids: &[String]) -> RoaringBitmap {
        uids.iter()
            .filter_map(|uid| self.uid_to_oid.get(uid.as_str()))
            .collect()
    }

    pub(crate) fn active_domain(&'me self) -> RoaringBitmap {
        self.uid_to_oid.values().collect()
    }
}

pub(crate) enum MetadataProvider<'me> {
    Compact(&'me MetadataSegmentReader<'me>),
    Log(&'me MetadataLogReader<'me>),
}

impl<'me> MetadataProvider<'me> {
    pub(crate) fn from_metadata_segment_reader(reader: &'me MetadataSegmentReader<'me>) -> Self {
        Self::Compact(reader)
    }

    pub(crate) fn from_metadata_log_reader(reader: &'me MetadataLogReader<'me>) -> Self {
        Self::Log(reader)
    }

    pub(crate) async fn filter_by_document(
        &self,
        query: &str,
    ) -> Result<RoaringBitmap, MetadataFilteringError> {
        use MetadataProvider::*;
        match self {
            Compact(metadata_segment_reader) => {
                if let Some(reader) = metadata_segment_reader.full_text_index_reader.as_ref() {
                    Ok(reader
                        .search(query)
                        .await
                        .map_err(MetadataIndexError::FullTextError)?)
                } else {
                    Ok(RoaringBitmap::new())
                }
            }
            Log(metadata_log_reader) => Ok(metadata_log_reader
                .document
                .iter()
                .filter_map(|(oid, doc)| doc.contains(query).then_some(oid))
                .collect()),
        }
    }

    pub(crate) async fn filter_by_metadata(
        &self,
        key: &str,
        val: &MetadataValue,
        op: &PrimitiveOperator,
    ) -> Result<RoaringBitmap, MetadataFilteringError> {
        use MetadataProvider::*;
        use MetadataValue::*;
        use PrimitiveOperator::*;
        match self {
            Compact(metadata_segment_reader) => {
                let (metadata_index_reader, kw) = match val {
                    Bool(b) => (
                        metadata_segment_reader.bool_metadata_index_reader.as_ref(),
                        &(*b).into(),
                    ),
                    Int(i) => (
                        metadata_segment_reader.u32_metadata_index_reader.as_ref(),
                        &(*i as u32).into(),
                    ),
                    Float(f) => (
                        metadata_segment_reader.f32_metadata_index_reader.as_ref(),
                        &(*f as f32).into(),
                    ),
                    Str(s) => (
                        metadata_segment_reader
                            .string_metadata_index_reader
                            .as_ref(),
                        &s.as_str().into(),
                    ),
                };
                if let Some(reader) = metadata_index_reader {
                    match op {
                        Equal => Ok(reader.get(key, kw).await?),
                        GreaterThan => Ok(reader.gt(key, kw).await?),
                        GreaterThanOrEqual => Ok(reader.gte(key, kw).await?),
                        LessThan => Ok(reader.lt(key, kw).await?),
                        LessThanOrEqual => Ok(reader.lte(key, kw).await?),
                        // Inequality filter is not supported at metadata provider level
                        NotEqual => Err(MetadataFilteringError::InvalidInput),
                    }
                } else {
                    Ok(RoaringBitmap::new())
                }
            }
            Log(metadata_log_reader) => metadata_log_reader.get(key, val, op),
        }
    }
}

pub(crate) trait RoaringMetadataFilter<'me> {
    async fn eval(
        &'me self,
        meta_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, MetadataFilteringError>;
}

impl<'me> RoaringMetadataFilter<'me> for Where {
    async fn eval(
        &'me self,
        meta_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, MetadataFilteringError> {
        use Where::*;
        match self {
            DirectWhereComparison(direct_comparison) => direct_comparison.eval(meta_provider).await,
            DirectWhereDocumentComparison(direct_document_comparison) => {
                direct_document_comparison.eval(meta_provider).await
            }
            WhereChildren(where_children) => Box::pin(where_children.eval(meta_provider)).await,
        }
    }
}

impl<'me> RoaringMetadataFilter<'me> for DirectWhereComparison {
    async fn eval(
        &'me self,
        meta_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, MetadataFilteringError> {
        use MetadataSetValue::*;
        use PrimitiveOperator::*;
        use SetOperator::*;
        use SignedRoaringBitmap::*;
        let result = match &self.comparison {
            WhereComparison::Primitive(primitive_operator, metadata_value) => {
                match primitive_operator {
                    // We convert the inequality check in to an equality check, and then negate the result
                    NotEqual => Exclude(
                        meta_provider
                            .filter_by_metadata(&self.key, metadata_value, &Equal)
                            .await?,
                    ),
                    Equal | GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual => {
                        Include(
                            meta_provider
                                .filter_by_metadata(&self.key, metadata_value, primitive_operator)
                                .await?,
                        )
                    }
                }
            }
            WhereComparison::Set(set_operator, metadata_set_value) => {
                let child_values: Vec<_> = match metadata_set_value {
                    Bool(vec) => vec.iter().map(|b| MetadataValue::Bool(*b)).collect(),
                    Int(vec) => vec.iter().map(|i| MetadataValue::Int(*i)).collect(),
                    Float(vec) => vec.iter().map(|f| MetadataValue::Float(*f)).collect(),
                    Str(vec) => vec.iter().map(|s| MetadataValue::Str(s.clone())).collect(),
                };
                let mut child_evals = Vec::with_capacity(child_values.len());
                for val in child_values {
                    let eval = meta_provider
                        .filter_by_metadata(&self.key, &val, &Equal)
                        .await?;
                    match set_operator {
                        In => child_evals.push(Include(eval)),
                        NotIn => child_evals.push(Exclude(eval)),
                    };
                }
                match set_operator {
                    In => child_evals
                        .into_iter()
                        .fold(SignedRoaringBitmap::empty(), BitOr::bitor),
                    NotIn => child_evals
                        .into_iter()
                        .fold(SignedRoaringBitmap::full(), BitAnd::bitand),
                }
            }
        };
        Ok(result)
    }
}

impl<'me> RoaringMetadataFilter<'me> for DirectDocumentComparison {
    async fn eval(
        &'me self,
        meta_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, MetadataFilteringError> {
        use DocumentOperator::*;
        use SignedRoaringBitmap::*;
        let contain = meta_provider
            .filter_by_document(self.document.as_str())
            .await?;
        match self.operator {
            Contains => Ok(Include(contain)),
            NotContains => Ok(Exclude(contain)),
        }
    }
}

impl<'me> RoaringMetadataFilter<'me> for WhereChildren {
    async fn eval(
        &'me self,
        meta_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, MetadataFilteringError> {
        use BooleanOperator::*;
        let mut child_evals = Vec::new();
        for child in &self.children {
            child_evals.push(child.eval(meta_provider).await?);
        }
        match self.operator {
            And => Ok(child_evals
                .into_iter()
                .fold(SignedRoaringBitmap::full(), BitAnd::bitand)),
            Or => Ok(child_evals
                .into_iter()
                .fold(SignedRoaringBitmap::empty(), BitOr::bitor)),
        }
    }
}

#[async_trait]
impl Operator<MetadataFilteringInput, MetadataFilteringOutput> for MetadataFilteringOperator {
    type Error = MetadataFilteringError;

    fn get_name(&self) -> &'static str {
        "MetadataFilteringOperator"
    }

    async fn run(
        &self,
        input: &MetadataFilteringInput,
    ) -> Result<MetadataFilteringOutput, MetadataFilteringError> {
        use SignedRoaringBitmap::*;
        trace!(
            "[MetadataFilteringOperator] segment id: {}",
            input.record_segment.id.to_string()
        );

        // Initialize record segment reader
        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            // Uninitialized segment is fine and means that the record
            // segment is not yet initialized in storage
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => {
                tracing::error!("Error creating record segment reader {}", e);
                Err(MetadataFilteringError::RecordSegmentReaderCreationError(*e))
            }
        }?;

        // Materialize the logs
        let materializer = LogMaterializer::new(
            record_segment_reader.clone(),
            input.log_record.clone(),
            None,
        );
        let materialized_logs = materializer
            .materialize()
            .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
            .await
            .map_err(|e| {
                tracing::error!("Error materializing log: {}", e);
                MetadataFilteringError::LogMaterializationError(e)
            })?;
        let metadata_log_reader = MetadataLogReader::new(&materialized_logs);
        let log_metadata_provider =
            MetadataProvider::from_metadata_log_reader(&metadata_log_reader);

        // Initialize metadata segment reader
        let metadata_segement_reader =
            MetadataSegmentReader::from_segment(&input.metadata_segment, &input.blockfile_provider)
                .await
                .map_err(MetadataFilteringError::MetadataSegmentReaderError)?;
        let compact_metadata_provider =
            MetadataProvider::from_metadata_segment_reader(&metadata_segement_reader);

        // Get offset ids corresponding to user ids
        let (user_log_oids, user_compact_oids) = if let Some(uids) = input.query_ids.as_ref() {
            let log_oids = Include(metadata_log_reader.search_user_ids(uids));
            let compact_oids = if let Some(reader) = record_segment_reader.as_ref() {
                let mut compact_oids = RoaringBitmap::new();
                for uid in uids {
                    if let Ok(oid) = reader.get_offset_id_for_user_id(uid.as_str()).await {
                        compact_oids.insert(oid);
                    }
                }
                Include(compact_oids)
            } else {
                SignedRoaringBitmap::full()
            };
            (log_oids, compact_oids)
        } else {
            (SignedRoaringBitmap::full(), SignedRoaringBitmap::full())
        };

        // Filter the offset ids in the log if the where clause is provided
        let filterd_log_oids = if let Some(clause) = input.where_clause.as_ref() {
            clause.eval(&log_metadata_provider).await? & user_log_oids
        } else {
            user_log_oids
        };

        // Materialize the offset ids to include from the log in the final result
        let materialized_log_oids = match filterd_log_oids {
            Include(rbm) => rbm,
            Exclude(rbm) => metadata_log_reader.active_domain() - rbm,
        };

        // Filter the offset ids in the metadata segment if the where clause is provided
        // This always exclude all offsets that is present in the materialized log
        let filtered_compact_oids = if let Some(clause) = input.where_clause.as_ref() {
            clause.eval(&compact_metadata_provider).await?
                & user_compact_oids
                & Exclude(metadata_log_reader.domain)
        } else {
            user_compact_oids & Exclude(metadata_log_reader.domain)
        };

        // Materialize the offset ids to include from the metadata segment in the final result
        // This should only contain offset ids not present in the materialized log
        let materialized_compact_oids = match filtered_compact_oids {
            Include(rbm) => rbm,
            Exclude(rbm) => {
                if let Some(reader) = record_segment_reader.as_ref() {
                    // TODO: Optimize for offset limit performance
                    reader
                        .get_all_offset_ids()
                        .await
                        .map_err(|_| MetadataFilteringError::RecordSegmentReaderError)?
                        - rbm
                } else {
                    RoaringBitmap::new()
                }
            }
        };

        // Merge the materialized offset ids from the log and from the metadata segment
        // The two roaring bitmaps involved here should be disjoint
        let mut merged_oids = materialized_compact_oids | materialized_log_oids;
        if let Some(skip) = input.offset.as_ref() {
            merged_oids.remove_smallest(*skip as u64);
        }

        if let Some(take) = input.limit.as_ref() {
            merged_oids = merged_oids.into_iter().take(*take as usize).collect();
        }

        Ok(MetadataFilteringOutput {
            log_records: input.log_record.clone(),
            offset_ids: merged_oids,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::execution::operator::Operator;
    use crate::{
        execution::operators::metadata_filtering::{
            MetadataFilteringInput, MetadataFilteringOperator,
        },
        segment::{
            metadata_segment::MetadataSegmentWriter,
            record_segment::{
                RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
            },
            types::SegmentFlusher,
            LogMaterializer, SegmentWriter,
        },
    };
    use chroma_blockstore::{
        arrow::{config::TEST_MAX_BLOCK_SIZE_BYTES, provider::ArrowBlockfileProvider},
        provider::BlockfileProvider,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        BooleanOperator, Chunk, CollectionUuid, DirectDocumentComparison, DirectWhereComparison,
        DocumentOperator, LogRecord, MetadataSetValue, MetadataValue, Operation, OperationRecord,
        PrimitiveOperator, SetOperator, UpdateMetadataValue, Where, WhereChildren, WhereComparison,
    };
    use roaring::RoaringBitmap;
    use std::{collections::HashMap, str::FromStr};
    use uuid::Uuid;

    #[tokio::test]
    async fn where_and_where_document_from_log() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut metadata_writer =
                MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut update_metadata = HashMap::new();
            update_metadata.insert(
                String::from("hello"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            update_metadata.insert(
                String::from("bye"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: Some(update_metadata.clone()),
                        document: Some(String::from("This is a document about cats.")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: Some(update_metadata),
                        document: Some(String::from("This is a document about dogs.")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReader>;
            match RecordSegmentReader::from_segment(&record_segment, &blockfile_provider).await {
                Ok(reader) => {
                    record_segment_reader = Some(reader);
                }
                Err(e) => {
                    match *e {
                        // Uninitialized segment is fine and means that the record
                        // segment is not yet initialized in storage.
                        RecordSegmentReaderCreationError::UninitializedSegment => {
                            record_segment_reader = None;
                        }
                        RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                            panic!("Error creating record segment reader");
                        }
                    };
                }
            };
            let materializer = LogMaterializer::new(record_segment_reader, data, None);
            let mat_records = materializer
                .materialize()
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(mat_records.clone())
                .await
                .expect("Apply materialized log to metadata segment failed");
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            metadata_writer
                .write_to_blockfiles()
                .await
                .expect("Metadata writer: write to blockfile failed");
            let record_flusher = segment_writer
                .commit()
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = record_flusher
                .flush()
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = metadata_flusher
                .flush()
                .await
                .expect("Flush metadata segment writer failed");
        }
        let mut update_metadata = HashMap::new();
        update_metadata.insert(
            String::from("hello"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        update_metadata.insert(
            String::from("hello_again"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        let data = vec![
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: Some(update_metadata.clone()),
                    document: None,
                    operation: Operation::Update,
                },
            },
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: Some(update_metadata),
                    document: Some(String::from("This is a document about dogs.")),
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 5,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: Some(vec![10.0, 11.0, 12.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Update,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let operator = MetadataFilteringOperator::new();
        let where_clause: Where = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("hello"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("new_world")),
            ),
        });
        let where_document_clause =
            Where::DirectWhereDocumentComparison(DirectDocumentComparison {
                document: String::from("about dogs"),
                operator: chroma_types::DocumentOperator::Contains,
            });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(Where::conjunction(vec![
                where_clause,
                where_document_clause,
            ])),
            None,
            None,
        );
        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");

        assert_eq!(1, res.offset_ids.len());
        assert_eq!(3, res.offset_ids.select(0).expect("Expect not none"));
    }

    #[tokio::test]
    async fn where_from_metadata_segment() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut metadata_writer =
                MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut update_metadata = HashMap::new();
            update_metadata.insert(
                String::from("hello"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            update_metadata.insert(
                String::from("bye"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: Some(update_metadata.clone()),
                        document: Some(String::from("This is a document about cats.")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: Some(update_metadata),
                        document: Some(String::from("This is a document about dogs.")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReader>;
            match RecordSegmentReader::from_segment(&record_segment, &blockfile_provider).await {
                Ok(reader) => {
                    record_segment_reader = Some(reader);
                }
                Err(e) => {
                    match *e {
                        // Uninitialized segment is fine and means that the record
                        // segment is not yet initialized in storage.
                        RecordSegmentReaderCreationError::UninitializedSegment => {
                            record_segment_reader = None;
                        }
                        RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                            panic!("Error creating record segment reader");
                        }
                    };
                }
            };
            let materializer = LogMaterializer::new(record_segment_reader, data, None);
            let mat_records = materializer
                .materialize()
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(mat_records.clone())
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .write_to_blockfiles()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = segment_writer
                .commit()
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = record_flusher
                .flush()
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = metadata_flusher
                .flush()
                .await
                .expect("Flush metadata segment writer failed");
        }
        let mut update_metadata = HashMap::new();
        update_metadata.insert(
            String::from("hello"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        update_metadata.insert(
            String::from("hello_again"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        let data = vec![LogRecord {
            log_offset: 3,
            record: OperationRecord {
                id: "embedding_id_3".to_string(),
                embedding: Some(vec![7.0, 8.0, 9.0]),
                encoding: None,
                metadata: Some(update_metadata),
                document: Some(String::from("This is a document about dogs.")),
                operation: Operation::Add,
            },
        }];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let operator = MetadataFilteringOperator::new();
        let where_clause: Where = Where::DirectWhereComparison(DirectWhereComparison {
            key: String::from("bye"),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str(String::from("world")),
            ),
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_clause),
            None,
            None,
        );
        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");

        assert_eq!(2, res.offset_ids.len());
        // Already sorted.
        assert_eq!(
            1,
            res.offset_ids.select(0).expect("Expected not none value")
        );
        assert_eq!(
            2,
            res.offset_ids.select(1).expect("Expected not none value")
        );
    }

    #[tokio::test]
    async fn query_ids_only() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let metadata_writer =
                MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut update_metadata = HashMap::new();
            update_metadata.insert(
                String::from("hello"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            update_metadata.insert(
                String::from("bye"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: Some(update_metadata.clone()),
                        document: Some(String::from("This is a document about cats.")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: Some(update_metadata),
                        document: Some(String::from("This is a document about dogs.")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReader>;
            match RecordSegmentReader::from_segment(&record_segment, &blockfile_provider).await {
                Ok(reader) => {
                    record_segment_reader = Some(reader);
                }
                Err(e) => {
                    match *e {
                        // Uninitialized segment is fine and means that the record
                        // segment is not yet initialized in storage.
                        RecordSegmentReaderCreationError::UninitializedSegment => {
                            record_segment_reader = None;
                        }
                        RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                            panic!("Error creating record segment reader");
                        }
                    };
                }
            };
            let materializer = LogMaterializer::new(record_segment_reader, data, None);
            let mat_records = materializer
                .materialize()
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(mat_records.clone())
                .await
                .expect("Apply materialized log to metadata segment failed");
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = segment_writer
                .commit()
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = record_flusher
                .flush()
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = metadata_flusher
                .flush()
                .await
                .expect("Flush metadata segment writer failed");
        }
        let mut update_metadata = HashMap::new();
        update_metadata.insert(
            String::from("hello"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        update_metadata.insert(
            String::from("hello_again"),
            UpdateMetadataValue::Str(String::from("new_world")),
        );
        let data = vec![
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: Some(update_metadata.clone()),
                    document: None,
                    operation: Operation::Update,
                },
            },
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![7.0, 8.0, 9.0]),
                    encoding: None,
                    metadata: Some(update_metadata),
                    document: Some(String::from("This is a document about dogs.")),
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 5,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: Some(vec![10.0, 11.0, 12.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Update,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let operator = MetadataFilteringOperator::new();
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            Some(vec![
                String::from("embedding_id_1"),
                String::from("embedding_id_3"),
            ]),
            None,
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(2, res.offset_ids.len());
        assert_eq!(1, res.offset_ids.select(0).expect("Expect not none value"));
        assert_eq!(3, res.offset_ids.select(1).expect("Expect not none value"));
    }

    #[tokio::test]
    async fn test_composite_filter() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut metadata_writer =
                MetadataSegmentWriter::from_segment(&metadata_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let mut logs = Vec::new();
            for i in 1..=60 {
                let mut meta = HashMap::new();
                if i % 2 == 0 {
                    meta.insert("even".to_string(), UpdateMetadataValue::Bool(i % 4 == 0));
                }
                meta.insert(
                    format!("mod_three_{}", i % 3),
                    UpdateMetadataValue::Float(i as f64),
                );
                meta.insert("mod_five".to_string(), UpdateMetadataValue::Int(i % 5));
                let emb = (0..3).map(|o| (3 * i + o) as f32).collect();
                logs.push(LogRecord {
                    log_offset: i,
                    record: OperationRecord {
                        id: format!("id_{}", i),
                        embedding: Some(emb),
                        encoding: None,
                        metadata: Some(meta),
                        document: Some(format!("-->{}<--", i)),
                        operation: Operation::Add,
                    },
                });
            }
            let data: Chunk<LogRecord> = Chunk::new(logs.into());
            let record_segment_reader: Option<RecordSegmentReader>;
            match RecordSegmentReader::from_segment(&record_segment, &blockfile_provider).await {
                Ok(reader) => {
                    record_segment_reader = Some(reader);
                }
                Err(e) => {
                    match *e {
                        // Uninitialized segment is fine and means that the record
                        // segment is not yet initialized in storage.
                        RecordSegmentReaderCreationError::UninitializedSegment => {
                            record_segment_reader = None;
                        }
                        RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                            panic!("Error creating record segment reader");
                        }
                    };
                }
            };
            let materializer = LogMaterializer::new(record_segment_reader, data, None);
            let mat_records = materializer
                .materialize()
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(mat_records.clone())
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .write_to_blockfiles()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = segment_writer
                .commit()
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = record_flusher
                .flush()
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = metadata_flusher
                .flush()
                .await
                .expect("Flush metadata segment writer failed");
        }
        let mut logs = Vec::new();
        for i in 61..=120 {
            let mut meta = HashMap::new();
            if i % 2 == 0 {
                meta.insert("even".to_string(), UpdateMetadataValue::Bool(i % 4 == 0));
            }
            meta.insert(
                format!("mod_three_{}", i % 3),
                UpdateMetadataValue::Float(i as f64),
            );
            meta.insert("mod_five".to_string(), UpdateMetadataValue::Int(i % 5));
            let emb = (0..3).map(|o| (3 * i + o) as f32).collect();
            logs.push(LogRecord {
                log_offset: i,
                record: OperationRecord {
                    id: format!("id_{}", i),
                    embedding: Some(emb),
                    encoding: None,
                    metadata: Some(meta),
                    document: Some(format!("-->{}<--", i)),
                    operation: Operation::Add,
                },
            });
        }
        for i in 1..=20 {
            logs.push(LogRecord {
                log_offset: 120 + i,
                record: OperationRecord {
                    id: format!("id_{}", i * 6),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                },
            });
        }
        let data: Chunk<LogRecord> = Chunk::new(logs.into());
        let operator = MetadataFilteringOperator::new();

        // Test set summary:
        // Total records count: 120, with id 1-120
        // Records 1-60 are compacted
        // Records 61-120 are in the log
        // Records with id % 6 == 1 are deleted
        // Record metadata has the following keys
        // - even: only exists for even ids, value is a boolean matching id % 4 == 0
        // - mod_three_{id % 3}: a floating point value converted from id
        // - mod_five: an integer value matching id % 5
        // Record document has format "-->{id}<--"

        let existing = (1..=120).filter(|i| i % 6 != 0);

        // A full scan should yield all existing records that are not yet deleted
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            None,
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(res.offset_ids, existing.clone().collect());

        // A full scan within the user specified ids should yield matching records
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            Some((31..=90).map(|i| format!("id_{}", i)).collect()),
            None,
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(
            res.offset_ids,
            existing.clone().filter(|i| (31..=90).contains(i)).collect()
        );

        // A $eq check on metadata should yield matching records
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: "mod_five".to_string(),
            comparison: WhereComparison::Primitive(PrimitiveOperator::Equal, MetadataValue::Int(2)),
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_clause),
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(
            res.offset_ids,
            existing.clone().filter(|i| i % 5 == 2).collect()
        );

        // A $ne check on metadata should yield matching records
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: "even".to_string(),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::NotEqual,
                MetadataValue::Bool(false),
            ),
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_clause),
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(
            res.offset_ids,
            existing
                .clone()
                .filter(|i| i % 2 == 1 || i % 4 == 0)
                .collect()
        );

        // A $lte check on metadata should yield matching records
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: "mod_three_2".to_string(),
            comparison: WhereComparison::Primitive(
                PrimitiveOperator::LessThanOrEqual,
                MetadataValue::Float(50.0),
            ),
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_clause),
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(
            res.offset_ids,
            existing
                .clone()
                .filter(|i| i % 3 == 2 && i <= &50)
                .collect()
        );

        // A $contains check on document should yield matching records
        let where_doc_clause = Where::DirectWhereDocumentComparison(DirectDocumentComparison {
            operator: DocumentOperator::Contains,
            document: String::from("6<-"),
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_doc_clause),
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(
            res.offset_ids,
            existing.clone().filter(|i| i % 10 == 6).collect()
        );

        // A $not_contains check on document should yield matching records
        let where_doc_clause = Where::DirectWhereDocumentComparison(DirectDocumentComparison {
            operator: DocumentOperator::NotContains,
            document: String::from("3<-"),
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_doc_clause),
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(
            res.offset_ids,
            existing.clone().filter(|i| i % 10 != 3).collect()
        );

        // A $in check on metadata should yield matching records
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: "mod_five".to_string(),
            comparison: WhereComparison::Set(SetOperator::In, MetadataSetValue::Int(vec![1, 3])),
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_clause),
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(
            res.offset_ids,
            existing
                .clone()
                .filter(|i| i % 5 == 1 || i % 5 == 3)
                .collect()
        );

        // A $in should behave like a disjunction of $eq
        let contain_res = res.offset_ids;
        let where_clause = Where::WhereChildren(WhereChildren {
            operator: BooleanOperator::Or,
            children: vec![
                Where::DirectWhereComparison(DirectWhereComparison {
                    key: "mod_five".to_string(),
                    comparison: WhereComparison::Primitive(
                        PrimitiveOperator::Equal,
                        MetadataValue::Int(1),
                    ),
                }),
                Where::DirectWhereComparison(DirectWhereComparison {
                    key: "mod_five".to_string(),
                    comparison: WhereComparison::Primitive(
                        PrimitiveOperator::Equal,
                        MetadataValue::Int(3),
                    ),
                }),
            ],
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_clause),
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(res.offset_ids, contain_res);

        // A $nin check on metadata should yield matching records
        let where_clause = Where::DirectWhereComparison(DirectWhereComparison {
            key: "mod_five".to_string(),
            comparison: WhereComparison::Set(SetOperator::NotIn, MetadataSetValue::Int(vec![1, 3])),
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_clause),
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(
            res.offset_ids,
            existing
                .clone()
                .filter(|i| i % 5 != 1 && i % 5 != 3)
                .collect()
        );

        // A $nin should behave like a conjunction of $neq
        let contain_res = res.offset_ids;
        let where_clause = Where::WhereChildren(WhereChildren {
            operator: BooleanOperator::And,
            children: vec![
                Where::DirectWhereComparison(DirectWhereComparison {
                    key: "mod_five".to_string(),
                    comparison: WhereComparison::Primitive(
                        PrimitiveOperator::NotEqual,
                        MetadataValue::Int(1),
                    ),
                }),
                Where::DirectWhereComparison(DirectWhereComparison {
                    key: "mod_five".to_string(),
                    comparison: WhereComparison::Primitive(
                        PrimitiveOperator::NotEqual,
                        MetadataValue::Int(3),
                    ),
                }),
            ],
        });
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            Some(where_clause),
            None,
            None,
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(res.offset_ids, contain_res);

        // offset and limit should yield the correct chunk of records
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            None,
            Some(36),
            Some(54),
        );
        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(res.offset_ids, existing.clone().skip(36).take(54).collect());

        // A large offset should yield no record
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            None,
            Some(200),
            None,
        );
        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(res.offset_ids, RoaringBitmap::new());

        // A large limit should yield all records
        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            None,
            None,
            None,
            Some(200),
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(res.offset_ids, existing.clone().collect());

        // Finally, test a composite filter with limit and offset
        let where_clause = Where::WhereChildren(WhereChildren {
            operator: BooleanOperator::And,
            children: vec![
                Where::DirectWhereComparison(DirectWhereComparison {
                    key: "mod_three_0".to_string(),
                    comparison: WhereComparison::Primitive(
                        PrimitiveOperator::GreaterThanOrEqual,
                        MetadataValue::Float(12.0),
                    ),
                }),
                Where::DirectWhereComparison(DirectWhereComparison {
                    key: "mod_five".to_string(),
                    comparison: WhereComparison::Set(
                        SetOperator::NotIn,
                        MetadataSetValue::Int(vec![0, 3]),
                    ),
                }),
                Where::WhereChildren(WhereChildren {
                    operator: BooleanOperator::Or,
                    children: vec![
                        Where::DirectWhereDocumentComparison(DirectDocumentComparison {
                            operator: DocumentOperator::NotContains,
                            document: "6<-".to_string(),
                        }),
                        Where::DirectWhereComparison(DirectWhereComparison {
                            key: "even".to_string(),
                            comparison: WhereComparison::Primitive(
                                PrimitiveOperator::Equal,
                                MetadataValue::Bool(true),
                            ),
                        }),
                    ],
                }),
            ],
        });

        let input = MetadataFilteringInput::new(
            blockfile_provider.clone(),
            record_segment.clone(),
            metadata_segment.clone(),
            data.clone(),
            Some((0..90).map(|i| format!("id_{}", i)).collect()),
            Some(where_clause),
            Some(2),
            Some(7),
        );

        let res = operator
            .run(&input)
            .await
            .expect("Error during running of operator");
        assert_eq!(
            res.offset_ids,
            existing
                .filter(|i| i % 3 == 0
                    && (12..=90).contains(i)
                    && i % 5 != 0
                    && i % 5 != 3
                    && (i % 10 != 6 || i % 4 == 0))
                .skip(2)
                .take(7)
                .collect()
        );
    }
}
