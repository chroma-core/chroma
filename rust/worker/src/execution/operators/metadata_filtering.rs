use std::{
    collections::BTreeMap,
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
    LogRecord, MaterializedLogOperation, MetadataValue, PrimitiveOperator, Segment, SetOperator,
    SignedRoaringBitmap, Where, WhereChildren, WhereComparison,
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
/// - `where_clause`: a boolean predicate on the metadata of the document.
/// - `where_document_clause`: a boolean predicate on the content of the document.
/// - `offset`: the number of records with smallest offset ids to skip, if specified
/// - `limit`: the number of records with smallest offset ids to take after the skip, if specified
///
/// # Output
/// - `log_record`: the same as `log_record` in the input.
/// - `log_mask`: the offset ids in the log that matches the criteria in the input.
/// - `offset_ids`: the offset_ids (in both log and compact storage) that matches the criteria in the input.
///
/// # Note
/// - The `MetadataProvider` enum can be viewed as an universal interface for the metadata and document index.
/// - In the input, `where_clause` and `where_document_clause` is represented with the same enum, as they share
///   the same evaluation process. In the future we can trivially merge them together into a single field.
/// - In the output, the `log_mask` should be a subset of `offset_ids`

#[derive(Debug)]
pub(crate) struct MetadataFilteringOperator {}

impl MetadataFilteringOperator {
    pub(crate) fn new() -> Box<Self> {
        Box::new(MetadataFilteringOperator {})
    }
}

#[derive(Debug)]
pub(crate) struct MetadataFilteringInput {
    blockfile_provider: BlockfileProvider,
    record_segment: Segment,
    metadata_segment: Segment,
    log_record: Chunk<LogRecord>,
    query_ids: Option<Vec<String>>,
    where_clause: Option<Where>,
    where_document_clause: Option<Where>,
    offset: Option<u32>,
    limit: Option<u32>,
}

impl MetadataFilteringInput {
    pub(crate) fn new(
        blockfile_provider: BlockfileProvider,
        record_segment: Segment,
        metadata_segment: Segment,
        log_record: Chunk<LogRecord>,
        query_ids: Option<Vec<String>>,
        where_clause: Option<Where>,
        where_document_clause: Option<Where>,
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
            where_document_clause,
            offset,
            limit,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MetadataFilteringOutput {
    pub(crate) log_records: Chunk<LogRecord>,
    pub(crate) log_mask: RoaringBitmap,
    pub(crate) offset_ids: RoaringBitmap,
}

#[derive(Error, Debug)]
pub(crate) enum MetadataFilteringError {
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

pub(crate) struct MetadataLogReader<'me> {
    compact_metadata: BTreeMap<&'me str, BTreeMap<MetadataValue, RoaringBitmap>>,
    document: BTreeMap<u32, &'me str>,
    domain: RoaringBitmap,
    uid_to_oid: BTreeMap<&'me str, u32>,
}

impl<'me> MetadataLogReader<'me> {
    pub(crate) fn new(logs: &'me Chunk<MaterializedLogRecord<'me>>) -> Self {
        let mut compact_metadata: BTreeMap<_, BTreeMap<_, RoaringBitmap>> = BTreeMap::new();
        let mut document = BTreeMap::new();
        let mut domain = RoaringBitmap::new();
        let mut uid_to_oid = BTreeMap::new();
        for (log, _) in logs.iter() {
            domain.insert(log.offset_id);
            uid_to_oid.insert(log.merged_user_id_ref(), log.offset_id);
            if !matches!(
                log.final_operation,
                MaterializedLogOperation::DeleteExisting
            ) {
                let log_meta = log.merged_metadata_ref();
                for (key, val) in log_meta.into_iter() {
                    compact_metadata
                        .entry(key)
                        .or_default()
                        .entry(val.clone())
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
                Equal => (Included(val), Included(val)),
                NotEqual => return Err(MetadataFilteringError::InvalidInput),
                GreaterThan => (Excluded(val), Unbounded),
                GreaterThanOrEqual => (Included(val), Unbounded),
                LessThan => (Unbounded, Excluded(val)),
                LessThanOrEqual => (Unbounded, Excluded(val)),
            };
            Ok(btm
                .range(bounds)
                .map(|(_, v)| v)
                .fold(RoaringBitmap::new(), BitOr::bitor))
        } else {
            Ok(RoaringBitmap::new())
        }
    }

    pub(crate) fn search_user_ids(&self, uids: &Vec<String>) -> RoaringBitmap {
        uids.into_iter()
            .filter_map(|uid| self.uid_to_oid.get(uid.as_str()))
            .collect()
    }

    pub(crate) fn domain(&'me self) -> &'me RoaringBitmap {
        &self.domain
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
                        .map_err(|e| MetadataIndexError::FullTextError(e))?)
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
                        _ => Err(MetadataFilteringError::InvalidInput),
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
        use PrimitiveOperator::*;
        use SetOperator::*;
        use SignedRoaringBitmap::*;
        let result = match &self.comp {
            WhereComparison::Primitive(primitive_operator, metadata_value) => {
                match primitive_operator {
                    NotEqual => Exclude(
                        meta_provider
                            .filter_by_metadata(&self.key, metadata_value, &Equal)
                            .await?,
                    ),
                    _ => Include(
                        meta_provider
                            .filter_by_metadata(&self.key, metadata_value, primitive_operator)
                            .await?,
                    ),
                }
            }
            WhereComparison::Set(set_operator, metadata_set_value) => match set_operator {
                In => {
                    Box::pin(
                        Where::conjunction(
                            metadata_set_value
                                .into_vec()
                                .into_iter()
                                .map(|mv| {
                                    Where::DirectWhereComparison(DirectWhereComparison {
                                        key: self.key.clone(),
                                        comp: WhereComparison::Primitive(Equal, mv.clone()),
                                    })
                                })
                                .collect(),
                        )
                        .eval(meta_provider),
                    )
                    .await?
                }
                NotIn => Box::pin(
                    (DirectWhereComparison {
                        key: self.key.clone(),
                        comp: WhereComparison::Set(In, metadata_set_value.clone()),
                    })
                    .eval(meta_provider),
                )
                .await?
                .flip(),
            },
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
            // segment is not yet initialized in storage.
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
                .map_err(|e| MetadataFilteringError::MetadataSegmentReaderError(e))?;
        let compact_metadata_provider =
            MetadataProvider::from_metadata_segment_reader(&metadata_segement_reader);

        let conjunction: Where;
        let clause = match (&input.where_clause, &input.where_document_clause) {
            (Some(wc), Some(wdc)) => {
                conjunction = Where::conjunction(vec![wc.clone(), wdc.clone()]);
                &conjunction
            }
            (Some(c), None) | (None, Some(c)) => c,
            _ => {
                conjunction = Where::conjunction(vec![]);
                &conjunction
            }
        };

        // Filter on log and compact segment, then merge the results with user provided ids
        let log_domain = metadata_log_reader.domain().clone();
        let mut filtered_log_oids = clause.eval(&log_metadata_provider).await?;
        let mut filtered_compact_oids =
            clause.eval(&compact_metadata_provider).await? & Exclude(log_domain.clone());

        if let Some(uids) = input.query_ids.as_ref() {
            filtered_log_oids =
                filtered_log_oids & Include(metadata_log_reader.search_user_ids(uids));
            if let Some(reader) = record_segment_reader.as_ref() {
                let mut compact_oids = RoaringBitmap::new();
                for uid in uids {
                    if let Ok(oid) = reader.get_offset_id_for_user_id(uid.as_str()).await {
                        compact_oids.insert(oid);
                    }
                }
                filtered_compact_oids = filtered_compact_oids & Include(compact_oids);
            }
        }

        let materialized_log_oids = match filtered_log_oids {
            Include(rbm) => rbm,
            Exclude(rbm) => log_domain - rbm,
        };

        let materialized_compact_oids = match filtered_compact_oids {
            Include(rbm) => rbm,
            Exclude(rbm) => {
                if let Some(reader) = record_segment_reader.as_ref() {
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

        let mut merged_oids = materialized_log_oids.clone() | materialized_compact_oids;
        if let Some(skip) = input.offset.as_ref() {
            merged_oids.remove_smallest(*skip as u64);
        }

        if let Some(take) = input.limit.as_ref() {
            let size = merged_oids.len();
            merged_oids.remove_biggest(size - (*take as u64).min(size));
        }

        let log_mask = materialized_log_oids & merged_oids.clone();

        Ok(MetadataFilteringOutput {
            log_records: input.log_record.clone(),
            log_mask,
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
    use chroma_cache::{cache::Cache, config::CacheConfig, config::UnboundedCacheConfig};
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        Chunk, DirectDocumentComparison, DirectWhereComparison, LogRecord, MetadataValue,
        Operation, OperationRecord, PrimitiveOperator, UpdateMetadataValue, Where, WhereComparison,
    };
    use std::{collections::HashMap, str::FromStr};
    use uuid::Uuid;

    #[tokio::test]
    async fn where_and_where_document_from_log() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
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
            collection: Uuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: Uuid::from_str("00000000-0000-0000-0000-000000000000")
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
            let mut record_segment_reader: Option<RecordSegmentReader> = None;
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
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
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
            comp: WhereComparison::Primitive(
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
            Some(where_clause),
            Some(where_document_clause),
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
        let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
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
            collection: Uuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: Uuid::from_str("00000000-0000-0000-0000-000000000000")
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
            let mut record_segment_reader: Option<RecordSegmentReader> = None;
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
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
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
            comp: WhereComparison::Primitive(
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
        let block_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
        let sparse_index_cache = Cache::new(&CacheConfig::Unbounded(UnboundedCacheConfig {}));
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
            collection: Uuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: Uuid::from_str("00000000-0000-0000-0000-000000000000")
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
            let mut record_segment_reader: Option<RecordSegmentReader> = None;
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
                .expect("Commit for segment writer failed");
            let metadata_flusher = metadata_writer
                .commit()
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
    async fn test_limit_offset() {}
}
