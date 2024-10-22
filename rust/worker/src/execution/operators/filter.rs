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
///
/// # Output
/// - `log_record`: the same `log_record` from the input.
/// - `log_oids`: the offset ids to include or exclude from the log.
/// - `compact_oids`: the offset ids to include or exclude from the record segment.
///
/// # Note
/// - The `MetadataProvider` enum can be viewed as an universal interface for the metadata and document index.
/// - In the output, `log_mask` should be a subset of `offset_ids`

#[derive(Debug)]
pub struct FilterOperator {}

impl FilterOperator {
    pub fn new() -> Box<Self> {
        Box::new(FilterOperator {})
    }
}

#[derive(Debug)]
pub struct FilterInput {
    blockfile_provider: BlockfileProvider,
    record_segment: Segment,
    metadata_segment: Segment,
    log_record: Chunk<LogRecord>,
    query_ids: Option<Vec<String>>,
    where_clause: Option<Where>,
}

impl FilterInput {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        record_segment: Segment,
        metadata_segment: Segment,
        log_record: Chunk<LogRecord>,
        query_ids: Option<Vec<String>>,
        where_clause: Option<Where>,
    ) -> Self {
        Self {
            blockfile_provider,
            record_segment,
            metadata_segment,
            log_record,
            query_ids,
            where_clause,
        }
    }
}

#[derive(Debug)]
pub struct FilterOutput {
    pub log_records: Chunk<LogRecord>,
    pub log_oids: SignedRoaringBitmap,
    pub compact_oids: SignedRoaringBitmap,
}

#[derive(Error, Debug)]
pub enum FilterError {
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

impl ChromaError for FilterError {
    fn code(&self) -> ErrorCodes {
        match self {
            FilterError::RecordSegmentReaderCreationError(e) => e.code(),
            FilterError::LogMaterializationError(e) => e.code(),
            FilterError::IndexError(e) => e.code(),
            FilterError::MetadataSegmentReaderError(e) => e.code(),
            FilterError::RecordSegmentReaderError => ErrorCodes::Internal,
            FilterError::InvalidInput => ErrorCodes::InvalidArgument,
        }
    }
}

/// This sturct provides an abstraction over the materialized logs that is similar to the metadata segment
pub(crate) struct MetadataLogReader<'me> {
    // This maps metadata keys to `BTreeMap`s, which further map values to offset ids
    // This mimics the layout in the metadata segment
    // //TODO: Maybe a sorted vector with binary search is more lightweight and performant?
    compact_metadata: HashMap<&'me str, BTreeMap<&'me MetadataValue, RoaringBitmap>>,
    // This maps offset ids to documents, excluding deleted ones
    document: HashMap<u32, &'me str>,
    // This contains all existing offset ids that are touched by the logs
    touched_oids: RoaringBitmap,
    // This maps user ids to offset ids, excluding deleted ones
    uid_to_oid: HashMap<&'me str, u32>,
}

impl<'me> MetadataLogReader<'me> {
    pub(crate) fn new(logs: &'me Chunk<MaterializedLogRecord<'me>>) -> Self {
        let mut compact_metadata: HashMap<_, BTreeMap<&MetadataValue, RoaringBitmap>> =
            HashMap::new();
        let mut document = HashMap::new();
        let mut touched_oids = RoaringBitmap::new();
        let mut uid_to_oid = HashMap::new();
        for (log, _) in logs.iter() {
            if !matches!(
                log.final_operation,
                MaterializedLogOperation::Initial | MaterializedLogOperation::AddNew
            ) {
                touched_oids.insert(log.offset_id);
            }
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
            touched_oids,
            uid_to_oid,
        }
    }
    pub(crate) fn get(
        &self,
        key: &str,
        val: &MetadataValue,
        op: &PrimitiveOperator,
    ) -> Result<RoaringBitmap, FilterError> {
        if let Some(btm) = self.compact_metadata.get(key) {
            let bounds = match op {
                PrimitiveOperator::Equal => (Bound::Included(&val), Bound::Included(&val)),
                PrimitiveOperator::GreaterThan => (Bound::Excluded(&val), Bound::Unbounded),
                PrimitiveOperator::GreaterThanOrEqual => (Bound::Included(&val), Bound::Unbounded),
                PrimitiveOperator::LessThan => (Bound::Unbounded, Bound::Excluded(&val)),
                PrimitiveOperator::LessThanOrEqual => (Bound::Unbounded, Bound::Included(&val)),
                // Inequality filter is not supported at metadata provider level
                PrimitiveOperator::NotEqual => return Err(FilterError::InvalidInput),
            };
            Ok(btm
                .range::<&MetadataValue, _>(bounds)
                .map(|(_, v)| v)
                .fold(RoaringBitmap::new(), BitOr::bitor))
        } else {
            Ok(RoaringBitmap::new())
        }
    }

    pub(crate) fn search_user_ids(&self, uids: &[&str]) -> RoaringBitmap {
        uids.iter()
            .filter_map(|uid| self.uid_to_oid.get(uid))
            .collect()
    }
}

pub(crate) enum MetadataProvider<'me> {
    CompactData(&'me MetadataSegmentReader<'me>),
    Log(&'me MetadataLogReader<'me>),
}

impl<'me> MetadataProvider<'me> {
    pub(crate) fn from_metadata_segment_reader(reader: &'me MetadataSegmentReader<'me>) -> Self {
        Self::CompactData(reader)
    }

    pub(crate) fn from_metadata_log_reader(reader: &'me MetadataLogReader<'me>) -> Self {
        Self::Log(reader)
    }

    pub(crate) async fn filter_by_document(
        &self,
        query: &str,
    ) -> Result<RoaringBitmap, FilterError> {
        match self {
            MetadataProvider::CompactData(metadata_segment_reader) => {
                if let Some(reader) = metadata_segment_reader.full_text_index_reader.as_ref() {
                    Ok(reader
                        .search(query)
                        .await
                        .map_err(MetadataIndexError::FullTextError)?)
                } else {
                    Ok(RoaringBitmap::new())
                }
            }
            MetadataProvider::Log(metadata_log_reader) => Ok(metadata_log_reader
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
    ) -> Result<RoaringBitmap, FilterError> {
        match self {
            MetadataProvider::CompactData(metadata_segment_reader) => {
                let (metadata_index_reader, kw) = match val {
                    MetadataValue::Bool(b) => (
                        metadata_segment_reader.bool_metadata_index_reader.as_ref(),
                        &(*b).into(),
                    ),
                    MetadataValue::Int(i) => (
                        metadata_segment_reader.u32_metadata_index_reader.as_ref(),
                        &(*i as u32).into(),
                    ),
                    MetadataValue::Float(f) => (
                        metadata_segment_reader.f32_metadata_index_reader.as_ref(),
                        &(*f as f32).into(),
                    ),
                    MetadataValue::Str(s) => (
                        metadata_segment_reader
                            .string_metadata_index_reader
                            .as_ref(),
                        &s.as_str().into(),
                    ),
                };
                if let Some(reader) = metadata_index_reader {
                    match op {
                        PrimitiveOperator::Equal => Ok(reader.get(key, kw).await?),
                        PrimitiveOperator::GreaterThan => Ok(reader.gt(key, kw).await?),
                        PrimitiveOperator::GreaterThanOrEqual => Ok(reader.gte(key, kw).await?),
                        PrimitiveOperator::LessThan => Ok(reader.lt(key, kw).await?),
                        PrimitiveOperator::LessThanOrEqual => Ok(reader.lte(key, kw).await?),
                        // Inequality filter is not supported at metadata provider level
                        PrimitiveOperator::NotEqual => Err(FilterError::InvalidInput),
                    }
                } else {
                    Ok(RoaringBitmap::new())
                }
            }
            MetadataProvider::Log(metadata_log_reader) => metadata_log_reader.get(key, val, op),
        }
    }
}

pub(crate) trait RoaringMetadataFilter<'me> {
    async fn eval(
        &'me self,
        meta_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, FilterError>;
}

impl<'me> RoaringMetadataFilter<'me> for Where {
    async fn eval(
        &'me self,
        meta_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, FilterError> {
        match self {
            Where::DirectWhereComparison(direct_comparison) => {
                direct_comparison.eval(meta_provider).await
            }
            Where::DirectWhereDocumentComparison(direct_document_comparison) => {
                direct_document_comparison.eval(meta_provider).await
            }
            Where::WhereChildren(where_children) => {
                // Box::pin is required to avoid infinite size future when recurse in async
                Box::pin(where_children.eval(meta_provider)).await
            }
        }
    }
}

impl<'me> RoaringMetadataFilter<'me> for DirectWhereComparison {
    async fn eval(
        &'me self,
        meta_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, FilterError> {
        let result = match &self.comparison {
            WhereComparison::Primitive(primitive_operator, metadata_value) => {
                match primitive_operator {
                    // We convert the inequality check in to an equality check, and then negate the result
                    PrimitiveOperator::NotEqual => SignedRoaringBitmap::Exclude(
                        meta_provider
                            .filter_by_metadata(
                                &self.key,
                                metadata_value,
                                &PrimitiveOperator::Equal,
                            )
                            .await?,
                    ),
                    PrimitiveOperator::Equal
                    | PrimitiveOperator::GreaterThan
                    | PrimitiveOperator::GreaterThanOrEqual
                    | PrimitiveOperator::LessThan
                    | PrimitiveOperator::LessThanOrEqual => SignedRoaringBitmap::Include(
                        meta_provider
                            .filter_by_metadata(&self.key, metadata_value, primitive_operator)
                            .await?,
                    ),
                }
            }
            WhereComparison::Set(set_operator, metadata_set_value) => {
                let child_values: Vec<_> = match metadata_set_value {
                    MetadataSetValue::Bool(vec) => {
                        vec.iter().map(|b| MetadataValue::Bool(*b)).collect()
                    }
                    MetadataSetValue::Int(vec) => {
                        vec.iter().map(|i| MetadataValue::Int(*i)).collect()
                    }
                    MetadataSetValue::Float(vec) => {
                        vec.iter().map(|f| MetadataValue::Float(*f)).collect()
                    }
                    MetadataSetValue::Str(vec) => {
                        vec.iter().map(|s| MetadataValue::Str(s.clone())).collect()
                    }
                };
                let mut child_evals = Vec::with_capacity(child_values.len());
                for val in child_values {
                    let eval = meta_provider
                        .filter_by_metadata(&self.key, &val, &PrimitiveOperator::Equal)
                        .await?;
                    match set_operator {
                        SetOperator::In => child_evals.push(SignedRoaringBitmap::Include(eval)),
                        SetOperator::NotIn => child_evals.push(SignedRoaringBitmap::Exclude(eval)),
                    };
                }
                match set_operator {
                    SetOperator::In => child_evals
                        .into_iter()
                        .fold(SignedRoaringBitmap::empty(), BitOr::bitor),
                    SetOperator::NotIn => child_evals
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
    ) -> Result<SignedRoaringBitmap, FilterError> {
        let contain = meta_provider
            .filter_by_document(self.document.as_str())
            .await?;
        match self.operator {
            DocumentOperator::Contains => Ok(SignedRoaringBitmap::Include(contain)),
            DocumentOperator::NotContains => Ok(SignedRoaringBitmap::Exclude(contain)),
        }
    }
}

impl<'me> RoaringMetadataFilter<'me> for WhereChildren {
    async fn eval(
        &'me self,
        meta_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, FilterError> {
        let mut child_evals = Vec::new();
        for child in &self.children {
            child_evals.push(child.eval(meta_provider).await?);
        }
        match self.operator {
            BooleanOperator::And => Ok(child_evals
                .into_iter()
                .fold(SignedRoaringBitmap::full(), BitAnd::bitand)),
            BooleanOperator::Or => Ok(child_evals
                .into_iter()
                .fold(SignedRoaringBitmap::empty(), BitOr::bitor)),
        }
    }
}

#[async_trait]
impl Operator<FilterInput, FilterOutput> for FilterOperator {
    type Error = FilterError;

    fn get_name(&self) -> &'static str {
        "FilterOperator"
    }

    async fn run(&self, input: &FilterInput) -> Result<FilterOutput, FilterError> {
        trace!(
            "[FilterOperator] segment id: {}",
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
                Err(FilterError::RecordSegmentReaderCreationError(*e))
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
                FilterError::LogMaterializationError(e)
            })?;
        let metadata_log_reader = MetadataLogReader::new(&materialized_logs);
        let log_metadata_provider =
            MetadataProvider::from_metadata_log_reader(&metadata_log_reader);

        // Initialize metadata segment reader
        let metadata_segement_reader =
            MetadataSegmentReader::from_segment(&input.metadata_segment, &input.blockfile_provider)
                .await
                .map_err(FilterError::MetadataSegmentReaderError)?;
        let compact_metadata_provider =
            MetadataProvider::from_metadata_segment_reader(&metadata_segement_reader);

        // Get offset ids corresponding to user ids
        let (user_log_oids, user_compact_oids) = if let Some(uids) = input.query_ids.as_ref() {
            let log_oids = SignedRoaringBitmap::Include(
                metadata_log_reader.search_user_ids(
                    uids.iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .as_slice(),
                ),
            );
            let compact_oids = if let Some(reader) = record_segment_reader.as_ref() {
                let mut compact_oids = RoaringBitmap::new();
                for uid in uids {
                    if let Ok(oid) = reader.get_offset_id_for_user_id(uid.as_str()).await {
                        compact_oids.insert(oid);
                    }
                }
                SignedRoaringBitmap::Include(compact_oids)
            } else {
                SignedRoaringBitmap::full()
            };
            (log_oids, compact_oids)
        } else {
            (SignedRoaringBitmap::full(), SignedRoaringBitmap::full())
        };

        // Filter the offset ids in the log if the where clause is provided
        let log_oids = if let Some(clause) = input.where_clause.as_ref() {
            clause.eval(&log_metadata_provider).await? & user_log_oids
        } else {
            user_log_oids
        };

        // Filter the offset ids in the metadata segment if the where clause is provided
        // This always exclude all offsets that is present in the materialized log
        let compact_oids = if let Some(clause) = input.where_clause.as_ref() {
            clause.eval(&compact_metadata_provider).await?
                & user_compact_oids
                & SignedRoaringBitmap::Exclude(metadata_log_reader.touched_oids)
        } else {
            user_compact_oids & SignedRoaringBitmap::Exclude(metadata_log_reader.touched_oids)
        };

        Ok(FilterOutput {
            log_records: input.log_record.clone(),
            log_oids,
            compact_oids,
        })
    }
}
