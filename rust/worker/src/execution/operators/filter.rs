use std::{
    collections::{BTreeMap, HashMap},
    ops::{BitAnd, BitOr, Bound},
};

use async_trait::async_trait;
use chroma_blockstore::{key::KeyWrapper, provider::BlockfileProvider};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::metadata::types::MetadataIndexError;
use chroma_segment::{
    blockfile_metadata::{MetadataSegmentError, MetadataSegmentReader},
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError, MaterializeLogsResult},
};
use chroma_system::Operator;
use chroma_types::{
    operator::Filter,
    regex::{
        literal_expr::{LiteralExpr, NgramLiteralProvider},
        ChromaRegex, ChromaRegexError,
    },
    BooleanOperator, Chunk, CompositeExpression, DataRecord, DocumentExpression, DocumentOperator,
    LogRecord, MaterializedLogOperation, MetadataComparison, MetadataExpression, MetadataSetValue,
    MetadataValue, PrimitiveOperator, Segment, SetOperator, SignedRoaringBitmap, Where,
};
use futures::future::try_join_all;
use roaring::RoaringBitmap;
use thiserror::Error;
use tracing::{Instrument, Span};

/// The `Filter` operator filters the collection with specified criteria
///
/// # Inputs
/// - `logs`: The latest log of the collection
/// - `blockfile_provider`: The blockfile provider
/// - `metadata_segment`: The metadata segment information
/// - `record_segment`: The record segment information
///
/// # Outputs
/// - `log_offset_ids`: The offset ids in the logs to include or exclude
/// - `compact_offset_ids`: The offset ids in the blockfile to include or exclude
///   All offsets ids present in the logs should be excluded in `compact_offset_ids`
///
/// # Usage
/// It can be used to derive the mask of offset ids that should be included or excluded by the next operator
#[derive(Clone, Debug)]
pub struct FilterInput {
    pub logs: Chunk<LogRecord>,
    pub blockfile_provider: BlockfileProvider,
    pub metadata_segment: Segment,
    pub record_segment: Segment,
}

#[derive(Clone, Debug)]
pub struct FilterOutput {
    pub log_offset_ids: SignedRoaringBitmap,
    pub compact_offset_ids: SignedRoaringBitmap,
}

#[derive(Error, Debug)]
pub enum FilterError {
    #[error("Error reading metadata index: {0}")]
    Index(#[from] MetadataIndexError),
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating metadata segment reader: {0}")]
    MetadataReader(#[from] MetadataSegmentError),
    #[error("Error getting record: {0}")]
    Record(#[from] Box<dyn ChromaError>),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error parsing regular expression: {0}")]
    Regex(#[from] ChromaRegexError),
}

impl ChromaError for FilterError {
    fn code(&self) -> ErrorCodes {
        match self {
            FilterError::Index(e) => e.code(),
            FilterError::LogMaterializer(e) => e.code(),
            FilterError::MetadataReader(e) => e.code(),
            FilterError::Record(e) => e.code(),
            FilterError::RecordReader(e) => e.code(),
            FilterError::Regex(_) => ErrorCodes::InvalidArgument,
        }
    }
}

/// This sturct provides an abstraction over the materialized logs that is similar to the metadata segment
pub(crate) struct MetadataLogReader<'me> {
    // This maps metadata keys to `BTreeMap`s, which further map values to offset ids
    // This mimics the layout in the metadata segment
    // //TODO: Maybe a sorted vector with binary search is more lightweight and performant?
    compact_metadata: HashMap<String, BTreeMap<MetadataValue, RoaringBitmap>>,
    // This maps offset ids to documents, excluding deleted ones
    document: HashMap<u32, &'me str>,
    // This contains all existing offset ids that are touched by the logs
    updated_offset_ids: RoaringBitmap,
    // This maps user ids to offset ids, excluding deleted ones
    user_id_to_offset_id: HashMap<&'me str, u32>,
}

impl<'me> MetadataLogReader<'me> {
    pub(crate) async fn create(
        logs: &'me MaterializeLogsResult,
        record_segment_reader: &'me Option<RecordSegmentReader<'me>>,
    ) -> Result<Self, LogMaterializerError> {
        let mut compact_metadata: HashMap<String, BTreeMap<MetadataValue, RoaringBitmap>> =
            HashMap::new();
        let mut document = HashMap::new();
        let mut updated_offset_ids = RoaringBitmap::new();
        let mut user_id_to_offset_id = HashMap::new();

        for log in logs {
            if !matches!(
                log.get_operation(),
                MaterializedLogOperation::Initial | MaterializedLogOperation::AddNew
            ) {
                updated_offset_ids.insert(log.get_offset_id());
            }
            if !matches!(
                log.get_operation(),
                MaterializedLogOperation::DeleteExisting
            ) {
                let log = log.hydrate(record_segment_reader.as_ref()).await?;
                user_id_to_offset_id.insert(log.get_user_id(), log.get_offset_id());
                let log_metadata = log.merged_metadata();
                for (key, val) in log_metadata.into_iter() {
                    compact_metadata
                        .entry(key)
                        .or_default()
                        .entry(val)
                        .or_default()
                        .insert(log.get_offset_id());
                }
                if let Some(doc) = log.merged_document_ref() {
                    document.insert(log.get_offset_id(), doc);
                }
            }
        }
        Ok(Self {
            compact_metadata,
            document,
            updated_offset_ids,
            user_id_to_offset_id,
        })
    }
    pub(crate) fn get(
        &self,
        key: &str,
        val: &MetadataValue,
        op: &PrimitiveOperator,
    ) -> Result<RoaringBitmap, FilterError> {
        if matches!(op, PrimitiveOperator::Equal) && key == "#id" {
            if let MetadataValue::Str(user_id) = val {
                return Ok(self
                    .user_id_to_offset_id
                    .get(user_id.as_str())
                    .into_iter()
                    .collect());
            }
        }
        if let Some(metadata_value_to_offset_ids) = self.compact_metadata.get(key) {
            let bounds = match op {
                PrimitiveOperator::Equal => (Bound::Included(val), Bound::Included(val)),
                PrimitiveOperator::GreaterThan => (Bound::Excluded(val), Bound::Unbounded),
                PrimitiveOperator::GreaterThanOrEqual => (Bound::Included(val), Bound::Unbounded),
                PrimitiveOperator::LessThan => (Bound::Unbounded, Bound::Excluded(val)),
                PrimitiveOperator::LessThanOrEqual => (Bound::Unbounded, Bound::Included(val)),
                PrimitiveOperator::NotEqual => unreachable!(
                    "Inequality filter should be handled above the metadata provider level"
                ),
            };
            Ok(metadata_value_to_offset_ids
                .range(bounds)
                .map(|(_, v)| v)
                .fold(RoaringBitmap::new(), BitOr::bitor))
        } else {
            Ok(RoaringBitmap::new())
        }
    }

    pub(crate) fn search_user_ids(&self, user_ids: &[&str]) -> RoaringBitmap {
        user_ids
            .iter()
            .filter_map(|id| self.user_id_to_offset_id.get(id))
            .collect()
    }
}

pub(crate) enum MetadataProvider<'me> {
    CompactData(
        &'me MetadataSegmentReader<'me>,
        &'me Option<RecordSegmentReader<'me>>,
    ),
    Log(&'me MetadataLogReader<'me>),
}

impl MetadataProvider<'_> {
    pub(crate) async fn filter_by_document_contains(
        &self,
        query: &str,
    ) -> Result<RoaringBitmap, FilterError> {
        match self {
            MetadataProvider::CompactData(metadata_segment_reader, _) => {
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
                .filter_map(|(offset_id, document)| document.contains(query).then_some(offset_id))
                .collect()),
        }
    }

    pub(crate) async fn filter_by_document_regex(
        &self,
        query: &str,
    ) -> Result<SignedRoaringBitmap, FilterError> {
        let chroma_regex = ChromaRegex::try_from(query.to_string())?;
        match self {
            MetadataProvider::CompactData(metadata_segment_reader, record_segment_reader) => {
                if let (Some(fti_reader), Some(rec_reader)) = (
                    metadata_segment_reader.full_text_index_reader.as_ref(),
                    record_segment_reader,
                ) {
                    // The pattern can match empty string and thus match any document
                    if let Some(0) = chroma_regex.properties().minimum_len() {
                        return Ok(SignedRoaringBitmap::full());
                    }
                    let literal_expr = LiteralExpr::from(chroma_regex.hir().clone());
                    let approximate_matching_offset_ids = fti_reader
                        .match_literal_expression(&literal_expr)
                        .await
                        .map_err(MetadataIndexError::from)?;
                    let is_exact_match = chroma_regex.properties().look_set().is_empty()
                        && fti_reader.can_match_exactly(&literal_expr);
                    if is_exact_match {
                        Ok(approximate_matching_offset_ids
                            .map(SignedRoaringBitmap::Include)
                            .unwrap_or(SignedRoaringBitmap::full()))
                    } else {
                        let regex = chroma_regex.regex()?;
                        let mut exact_matching_offset_ids = RoaringBitmap::new();
                        match approximate_matching_offset_ids {
                            // Perform point lookup for potential matching documents is there is not too many of them
                            Some(offset_ids)
                                if offset_ids.len() < rec_reader.count().await? as u64 / 10 =>
                            {
                                let fetch_futures: Vec<_> =
                                    offset_ids
                                        .into_iter()
                                        .map(|id| {
                                            async move {
                                        let data = rec_reader.get_data_for_offset_id(id).await?;
                                        Ok::<(u32, Option<DataRecord>), Box<dyn ChromaError>>((
                                            id, data,
                                        ))
                                    }.instrument(Span::current())
                                        })
                                        .collect();
                                let data_results = try_join_all(fetch_futures).await?;
                                for (id, data_opt) in data_results {
                                    if data_opt.is_some_and(|rec| {
                                        rec.document.is_some_and(|doc| regex.is_match(doc))
                                    }) {
                                        exact_matching_offset_ids.insert(id);
                                    }
                                }
                            }
                            // Perform range scan of all documents
                            candidate_offsets => {
                                for (offset, record) in rec_reader.get_all_data().await? {
                                    if (candidate_offsets.is_none()
                                        || candidate_offsets
                                            .as_ref()
                                            .is_some_and(|offsets| offsets.contains(offset)))
                                        && record.document.is_some_and(|doc| regex.is_match(doc))
                                    {
                                        exact_matching_offset_ids.insert(offset);
                                    }
                                }
                            }
                        }

                        Ok(SignedRoaringBitmap::Include(exact_matching_offset_ids))
                    }
                } else {
                    Ok(SignedRoaringBitmap::empty())
                }
            }
            MetadataProvider::Log(metadata_log_reader) => {
                // The pattern can match empty string and thus match any document
                if let Some(0) = chroma_regex.properties().minimum_len() {
                    return Ok(SignedRoaringBitmap::full());
                }
                let regex = chroma_regex.regex()?;
                Ok(SignedRoaringBitmap::Include(
                    metadata_log_reader
                        .document
                        .iter()
                        .filter_map(|(offset_id, document)| {
                            regex.is_match(document).then_some(offset_id)
                        })
                        .collect(),
                ))
            }
        }
    }

    pub(crate) async fn filter_by_metadata(
        &self,
        key: &str,
        val: &MetadataValue,
        op: &PrimitiveOperator,
    ) -> Result<RoaringBitmap, FilterError> {
        match self {
            MetadataProvider::CompactData(metadata_segment_reader, record_segment_reader) => {
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
                    MetadataValue::SparseVector(_) => {
                        unimplemented!("Comparison with sparse vector is not supported")
                    }
                };
                if let Some(reader) = metadata_index_reader {
                    match op {
                        PrimitiveOperator::Equal => {
                            if key == "#id" {
                                if let KeyWrapper::String(user_id) = kw {
                                    return Ok(match record_segment_reader {
                                        Some(reader) => reader
                                            .get_offset_id_for_user_id(user_id)
                                            .await?
                                            .iter()
                                            .collect(),
                                        None => RoaringBitmap::new(),
                                    });
                                }
                            }
                            Ok(reader.get(key, kw).await?)
                        }
                        PrimitiveOperator::GreaterThan => Ok(reader.gt(key, kw).await?),
                        PrimitiveOperator::GreaterThanOrEqual => Ok(reader.gte(key, kw).await?),
                        PrimitiveOperator::LessThan => Ok(reader.lt(key, kw).await?),
                        PrimitiveOperator::LessThanOrEqual => Ok(reader.lte(key, kw).await?),
                        PrimitiveOperator::NotEqual => unreachable!(
                            "Inequality filter should be handled above the metadata provider level"
                        ),
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
        metadata_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, FilterError>;
}

impl<'me> RoaringMetadataFilter<'me> for Where {
    async fn eval(
        &'me self,
        metadata_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, FilterError> {
        match self {
            Where::Metadata(direct_comparison) => direct_comparison.eval(metadata_provider).await,
            Where::Document(direct_document_comparison) => {
                direct_document_comparison.eval(metadata_provider).await
            }
            Where::Composite(where_children) => {
                // Box::pin is required to avoid infinite size future when recurse in async
                Box::pin(where_children.eval(metadata_provider)).await
            }
        }
    }
}

impl<'me> RoaringMetadataFilter<'me> for MetadataExpression {
    async fn eval(
        &'me self,
        metadata_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, FilterError> {
        let result = match &self.comparison {
            MetadataComparison::Primitive(primitive_operator, metadata_value) => {
                match primitive_operator {
                    // We convert the inequality check in to an equality check, and then negate the result
                    PrimitiveOperator::NotEqual => SignedRoaringBitmap::Exclude(
                        metadata_provider
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
                        metadata_provider
                            .filter_by_metadata(&self.key, metadata_value, primitive_operator)
                            .await?,
                    ),
                }
            }
            MetadataComparison::Set(set_operator, metadata_set_value) => {
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
                let mut child_evaluations = Vec::with_capacity(child_values.len());
                for value in child_values {
                    let eval = metadata_provider
                        .filter_by_metadata(&self.key, &value, &PrimitiveOperator::Equal)
                        .await?;
                    match set_operator {
                        SetOperator::In => {
                            child_evaluations.push(SignedRoaringBitmap::Include(eval))
                        }
                        SetOperator::NotIn => {
                            child_evaluations.push(SignedRoaringBitmap::Exclude(eval))
                        }
                    };
                }
                match set_operator {
                    SetOperator::In => child_evaluations
                        .into_iter()
                        .fold(SignedRoaringBitmap::empty(), BitOr::bitor),
                    SetOperator::NotIn => child_evaluations
                        .into_iter()
                        .fold(SignedRoaringBitmap::full(), BitAnd::bitand),
                }
            }
        };
        Ok(result)
    }
}

impl<'me> RoaringMetadataFilter<'me> for DocumentExpression {
    async fn eval(
        &'me self,
        metadata_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, FilterError> {
        match self.operator {
            DocumentOperator::Contains => Ok(SignedRoaringBitmap::Include(
                metadata_provider
                    .filter_by_document_contains(self.pattern.as_str())
                    .await?,
            )),
            DocumentOperator::NotContains => Ok(SignedRoaringBitmap::Exclude(
                metadata_provider
                    .filter_by_document_contains(self.pattern.as_str())
                    .await?,
            )),
            DocumentOperator::Regex => Ok(metadata_provider
                .filter_by_document_regex(self.pattern.as_str())
                .await?),
            DocumentOperator::NotRegex => Ok(metadata_provider
                .filter_by_document_regex(self.pattern.as_str())
                .await?
                .flip()),
        }
    }
}

impl<'me> RoaringMetadataFilter<'me> for CompositeExpression {
    async fn eval(
        &'me self,
        metadata_provider: &MetadataProvider<'me>,
    ) -> Result<SignedRoaringBitmap, FilterError> {
        let mut child_evaluations = Vec::new();
        for child in &self.children {
            child_evaluations.push(child.eval(metadata_provider).await?);
        }
        match self.operator {
            BooleanOperator::And => Ok(child_evaluations
                .into_iter()
                .fold(SignedRoaringBitmap::full(), BitAnd::bitand)),
            BooleanOperator::Or => Ok(child_evaluations
                .into_iter()
                .fold(SignedRoaringBitmap::empty(), BitOr::bitor)),
        }
    }
}

#[async_trait]
impl Operator<FilterInput, FilterOutput> for Filter {
    type Error = FilterError;

    async fn run(&self, input: &FilterInput) -> Result<FilterOutput, FilterError> {
        tracing::debug!(
            "[{}]: Num log entries {:?}",
            self.get_name(),
            input.logs.len(),
        );

        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        ))
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;
        let cloned_record_segment_reader = record_segment_reader.clone();
        let materialized_logs =
            materialize_logs(&cloned_record_segment_reader, input.logs.clone(), None)
                .instrument(tracing::trace_span!(parent: Span::current(), "Materialize logs"))
                .await?;
        let metadata_log_reader =
            MetadataLogReader::create(&materialized_logs, &record_segment_reader)
                .await
                .map_err(FilterError::LogMaterializer)?;

        // Short-circuit if filter is none.
        if self.query_ids.is_none() && self.where_clause.is_none() {
            return Ok(FilterOutput {
                log_offset_ids: SignedRoaringBitmap::full(),
                compact_offset_ids: SignedRoaringBitmap::full()
                    & SignedRoaringBitmap::Exclude(metadata_log_reader.updated_offset_ids),
            });
        }

        let log_metadata_provider = MetadataProvider::Log(&metadata_log_reader);

        let metadata_segement_reader = Box::pin(MetadataSegmentReader::from_segment(
            &input.metadata_segment,
            &input.blockfile_provider,
        ))
        .await?;
        let compact_metadata_provider =
            MetadataProvider::CompactData(&metadata_segement_reader, &record_segment_reader);

        // Get offset ids corresponding to user ids
        let (user_allowed_log_offset_ids, user_allowed_compact_offset_ids) =
            if let Some(user_allowed_ids) = self.query_ids.as_ref() {
                let log_offset_ids = SignedRoaringBitmap::Include(
                    metadata_log_reader.search_user_ids(
                        user_allowed_ids
                            .iter()
                            .map(String::as_str)
                            .collect::<Vec<_>>()
                            .as_slice(),
                    ),
                );
                let compact_offset_ids = if let Some(reader) = record_segment_reader.as_ref() {
                    let mut offset_ids = RoaringBitmap::new();
                    for user_id in user_allowed_ids {
                        match reader.get_offset_id_for_user_id(user_id.as_str()).await {
                            Ok(Some(offset_id)) => {
                                offset_ids.insert(offset_id);
                            }
                            Ok(None) => {
                                // NOTE(rescrv):  We are filtering by user id, and there is no
                                // document.  Drop it.
                            }
                            Err(e) => {
                                return Err(e.into());
                            }
                        };
                    }
                    SignedRoaringBitmap::Include(offset_ids)
                } else {
                    SignedRoaringBitmap::full()
                };
                (log_offset_ids, compact_offset_ids)
            } else {
                (SignedRoaringBitmap::full(), SignedRoaringBitmap::full())
            };

        // Filter the offset ids in the log if the where clause is provided
        let log_offset_ids = if let Some(clause) = self.where_clause.as_ref() {
            clause.eval(&log_metadata_provider).await? & user_allowed_log_offset_ids
        } else {
            user_allowed_log_offset_ids
        };

        // Filter the offset ids in the metadata segment if the where clause is provided
        // This always exclude all offsets that is present in the materialized log
        let compact_offset_ids = if let Some(clause) = self.where_clause.as_ref() {
            clause.eval(&compact_metadata_provider).await?
                & user_allowed_compact_offset_ids
                & SignedRoaringBitmap::Exclude(metadata_log_reader.updated_offset_ids)
        } else {
            user_allowed_compact_offset_ids
                & SignedRoaringBitmap::Exclude(metadata_log_reader.updated_offset_ids)
        };

        Ok(FilterOutput {
            log_offset_ids,
            compact_offset_ids,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use chroma_blockstore::{
        arrow::{
            config::{BlockManagerConfig, TEST_MAX_BLOCK_SIZE_BYTES},
            provider::ArrowBlockfileProvider,
        },
        provider::BlockfileProvider,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_log::test::{add_delete_generator, int_as_id, LoadFromGenerator, LogGenerator};
    use chroma_segment::{
        blockfile_metadata::{MetadataSegmentReader, MetadataSegmentWriter},
        blockfile_record::{
            RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
        },
        test::TestDistributedSegment,
        types::materialize_logs,
    };
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_system::Operator;
    use chroma_types::{
        operator::Filter, BooleanOperator, Chunk, CollectionUuid, CompositeExpression,
        DatabaseUuid, DocumentExpression, LogRecord, MetadataComparison, MetadataExpression,
        MetadataSetValue, MetadataValue, Operation, OperationRecord, PrimitiveOperator,
        SegmentUuid, SetOperator, SignedRoaringBitmap, Where,
    };

    use crate::execution::operators::filter::{MetadataLogReader, MetadataProvider};

    use super::FilterInput;

    /// The unit tests for `Filter` operator uses the following test data
    /// It generates 120 log records, where the first 60 is compacted:
    /// - Log: Delete [11..=20], add [51..=100]
    /// - Compacted: Delete [1..=10] deletion, add [11..=50]
    async fn setup_filter_input() -> (TestDistributedSegment, FilterInput) {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(60, add_delete_generator)
            .await;
        let blockfile_provider = test_segment.blockfile_provider.clone();
        let metadata_segment = test_segment.metadata_segment.clone();
        let record_segment = test_segment.record_segment.clone();
        (
            test_segment,
            FilterInput {
                logs: add_delete_generator.generate_chunk(61..=120),
                blockfile_provider,
                metadata_segment,
                record_segment,
            },
        )
    }

    #[tokio::test]
    async fn test_trivial_filter() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let filter_operator = Filter {
            query_ids: None,
            where_clause: None,
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(filter_output.log_offset_ids, SignedRoaringBitmap::full());
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Exclude((11..=20).collect())
        );
    }

    #[tokio::test]
    async fn test_simple_user_allowed_ids() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let filter_operator = Filter {
            query_ids: Some((0..30).map(int_as_id).collect()),
            where_clause: None,
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(filter_output.log_offset_ids, SignedRoaringBitmap::empty());
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include((21..30).collect())
        );
    }

    #[tokio::test]
    async fn test_simple_eq() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_clause = Where::Metadata(MetadataExpression {
            key: "is_even".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Bool(true),
            ),
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Include((51..=100).filter(|offset| offset % 2 == 0).collect())
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include((21..=50).filter(|offset| offset % 2 == 0).collect())
        );
    }

    #[tokio::test]
    async fn test_simple_ne() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_clause = Where::Metadata(MetadataExpression {
            key: "modulo_3".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::NotEqual,
                MetadataValue::Int(0),
            ),
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Exclude((51..=100).filter(|offset| offset % 3 == 0).collect())
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Exclude(
                (21..=50)
                    .filter(|offset| offset % 3 == 0)
                    .chain(11..=20)
                    .collect()
            )
        );
    }

    #[tokio::test]
    async fn test_simple_in() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_clause = Where::Metadata(MetadataExpression {
            key: "is_even".to_string(),
            comparison: MetadataComparison::Set(
                SetOperator::In,
                MetadataSetValue::Bool(vec![false, true]),
            ),
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Include((51..=100).collect())
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include((21..=50).collect())
        );
    }

    #[tokio::test]
    async fn test_simple_nin() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_clause = Where::Metadata(MetadataExpression {
            key: "modulo_3".to_string(),
            comparison: MetadataComparison::Set(
                SetOperator::NotIn,
                MetadataSetValue::Int(vec![1, 2]),
            ),
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Exclude((51..=100).filter(|offset| offset % 3 != 0).collect())
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Exclude(
                (21..=50)
                    .filter(|offset| offset % 3 != 0)
                    .chain(11..=20)
                    .collect()
            )
        );
    }

    #[tokio::test]
    async fn test_simple_gt() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_clause = Where::Metadata(MetadataExpression {
            key: "id".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::GreaterThan,
                MetadataValue::Int(36),
            ),
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Include((51..=100).collect())
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include((37..=50).collect())
        );
    }

    #[tokio::test]
    async fn test_simple_contains() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_clause = Where::Document(DocumentExpression {
            operator: chroma_types::DocumentOperator::Contains,
            pattern: "<cat>".to_string(),
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Include((51..=100).filter(|offset| offset % 3 == 0).collect())
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include((21..=50).filter(|offset| offset % 3 == 0).collect())
        );
    }

    #[tokio::test]
    async fn test_simple_not_contains() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_clause = Where::Document(DocumentExpression {
            operator: chroma_types::DocumentOperator::NotContains,
            pattern: "<dog>".to_string(),
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Exclude((51..=100).filter(|offset| offset % 5 == 0).collect())
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Exclude(
                (21..=50)
                    .filter(|offset| offset % 5 == 0)
                    .chain(11..=20)
                    .collect()
            )
        );
    }

    #[tokio::test]
    async fn test_simple_and() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_sub_clause_1 = Where::Metadata(MetadataExpression {
            key: "id".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::GreaterThan,
                MetadataValue::Int(36),
            ),
        });

        let where_sub_clause_2 = Where::Metadata(MetadataExpression {
            key: "is_even".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Bool(false),
            ),
        });

        let where_clause = Where::Composite(CompositeExpression {
            operator: BooleanOperator::And,
            children: vec![where_sub_clause_1, where_sub_clause_2],
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Include((51..=100).filter(|offset| offset % 2 == 1).collect())
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include((37..=50).filter(|offset| offset % 2 == 1).collect())
        );
    }

    #[tokio::test]
    async fn test_simple_or() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_sub_clause_1 = Where::Metadata(MetadataExpression {
            key: "modulo_3".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Int(0),
            ),
        });

        let where_sub_clause_2 = Where::Metadata(MetadataExpression {
            key: "modulo_3".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Int(2),
            ),
        });

        let where_clause = Where::Composite(CompositeExpression {
            operator: BooleanOperator::Or,
            children: vec![where_sub_clause_1, where_sub_clause_2],
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Include((51..=100).filter(|offset| offset % 3 != 1).collect())
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include((21..=50).filter(|offset| offset % 3 != 1).collect())
        );
    }

    #[tokio::test]
    async fn test_complex_filter() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let where_sub_clause_1 = Where::Document(DocumentExpression {
            operator: chroma_types::DocumentOperator::NotContains,
            pattern: "<dog>".to_string(),
        });

        let where_sub_clause_2 = Where::Metadata(MetadataExpression {
            key: "id".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::LessThan,
                MetadataValue::Int(72),
            ),
        });

        let where_sub_clause_3 = Where::Metadata(MetadataExpression {
            key: "modulo_3".to_string(),
            comparison: MetadataComparison::Set(
                SetOperator::NotIn,
                MetadataSetValue::Int(vec![0, 1]),
            ),
        });

        let where_clause = Where::Composite(CompositeExpression {
            operator: BooleanOperator::And,
            children: vec![
                where_sub_clause_1,
                Where::Composite(CompositeExpression {
                    operator: BooleanOperator::Or,
                    children: vec![where_sub_clause_2, where_sub_clause_3],
                }),
            ],
        });

        let filter_operator = Filter {
            query_ids: Some((0..96).map(int_as_id).collect()),
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Include(
                (51..96)
                    .filter(|offset| offset % 5 != 0 && (offset < &72 || offset % 3 == 2))
                    .collect()
            )
        );
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include((21..=50).filter(|offset| offset % 5 != 0).collect())
        );
    }

    #[tokio::test]
    async fn test_id_filter_with_in_operator() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        // Test with #id using $in operator
        // This simulates: {"#id": {"$in": ["id_25", "id_35", "id_55", "id_75", "id_95"]}}
        let where_clause = Where::Metadata(MetadataExpression {
            key: "#id".to_string(),
            comparison: MetadataComparison::Set(
                SetOperator::In,
                MetadataSetValue::Str(vec![
                    "id_25".to_string(), // In compact segment
                    "id_35".to_string(), // In compact segment
                    "id_55".to_string(), // In log (added)
                    "id_75".to_string(), // In log (added)
                    "id_95".to_string(), // In log (added)
                ]),
            ),
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        // The log contains offset IDs 51-100 (after deletions of 11-20)
        // So id_55 -> offset 55, id_75 -> offset 75, id_95 -> offset 95
        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Include([55, 75, 95].iter().collect())
        );

        // The compact segment contains offset IDs 21-50 (11-20 were deleted in log)
        // So id_25 -> offset 25, id_35 -> offset 35
        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include([25, 35].iter().collect())
        );
    }

    #[tokio::test]
    async fn test_id_filter_with_metadata_combination() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        // Test combining #id filter with regular metadata filter
        // This simulates: {"#id": {"$in": ["id_30", "id_40", "id_60", "id_80"]}, "is_even": true}
        let id_clause = Where::Metadata(MetadataExpression {
            key: "#id".to_string(),
            comparison: MetadataComparison::Set(
                SetOperator::In,
                MetadataSetValue::Str(vec![
                    "id_30".to_string(), // offset 30, is_even=true, in compact
                    "id_40".to_string(), // offset 40, is_even=true, in compact
                    "id_60".to_string(), // offset 60, is_even=true, in log
                    "id_80".to_string(), // offset 80, is_even=true, in log
                ]),
            ),
        });

        let even_clause = Where::Metadata(MetadataExpression {
            key: "is_even".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Bool(true),
            ),
        });

        let where_clause = Where::Composite(CompositeExpression {
            operator: BooleanOperator::And,
            children: vec![id_clause, even_clause],
        });

        let filter_operator = Filter {
            query_ids: None,
            where_clause: Some(where_clause),
        };

        let filter_output = filter_operator
            .run(&filter_input)
            .await
            .expect("Filter should not fail");

        // All selected IDs have even offsets, so they should all pass the is_even filter
        assert_eq!(
            filter_output.log_offset_ids,
            SignedRoaringBitmap::Include([60, 80].iter().collect())
        );

        assert_eq!(
            filter_output.compact_offset_ids,
            SignedRoaringBitmap::Include([30, 40].iter().collect())
        );
    }

    #[tokio::test]
    async fn test_regex_empty_posting_list() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer = RecordSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &record_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating segment writer");
            let mut metadata_writer = MetadataSegmentWriter::from_segment(
                &tenant,
                &database_id,
                &metadata_segment,
                &blockfile_provider,
            )
            .await
            .expect("Error creating segment writer");
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("DEF")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("def")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReader> = match Box::pin(
                RecordSegmentReader::from_segment(&record_segment, &blockfile_provider),
            )
            .await
            {
                Ok(reader) => Some(reader),
                Err(e) => {
                    match *e {
                        // Uninitialized segment is fine and means that the record
                        // segment is not yet initialized in storage.
                        RecordSegmentReaderCreationError::UninitializedSegment => None,
                        RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                            panic!("Error creating record segment reader");
                        }
                        _ => {
                            panic!("Unexpected error creating record segment reader: {:?}", e);
                        }
                    }
                }
            };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
        }
        let data = vec![
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("abc")),
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                },
            },
        ];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
        let segment_writer = RecordSegmentWriter::from_segment(
            &tenant,
            &database_id,
            &record_segment,
            &blockfile_provider,
        )
        .await
        .expect("Error creating segment writer");
        let mut metadata_writer = MetadataSegmentWriter::from_segment(
            &tenant,
            &database_id,
            &metadata_segment,
            &blockfile_provider,
        )
        .await
        .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(&some_reader, data, None)
            .await
            .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &metadata_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");
        let record_segment_reader = Box::pin(RecordSegmentReader::from_segment(
            &record_segment,
            &blockfile_provider,
        ))
        .await
        .expect("Reader should be initialized by now");
        let some_reader = Some(record_segment_reader);
        let compact_metadata_provider =
            MetadataProvider::CompactData(&metadata_segment_reader, &some_reader);
        let res = compact_metadata_provider
            .filter_by_document_regex("(?i)def")
            .await
            .expect("Expected regex to work");
        assert_eq!(res, SignedRoaringBitmap::Include([1].into()));
    }

    #[tokio::test]
    async fn test_regex_short_circuit() {
        let (_test_segment, filter_input) = setup_filter_input().await;

        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &filter_input.record_segment,
            &filter_input.blockfile_provider,
        ))
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }
        .unwrap();
        let cloned_record_segment_reader = record_segment_reader.clone();
        let materialized_logs = materialize_logs(
            &cloned_record_segment_reader,
            filter_input.logs.clone(),
            None,
        )
        .await
        .unwrap();
        let metadata_log_reader =
            MetadataLogReader::create(&materialized_logs, &record_segment_reader)
                .await
                .unwrap();
        let log_metadata_provider = MetadataProvider::Log(&metadata_log_reader);

        let metadata_segement_reader = Box::pin(MetadataSegmentReader::from_segment(
            &filter_input.metadata_segment,
            &filter_input.blockfile_provider,
        ))
        .await
        .unwrap();
        let compact_metadata_provider =
            MetadataProvider::CompactData(&metadata_segement_reader, &record_segment_reader);

        let match_all = r".*";
        assert_eq!(
            log_metadata_provider
                .filter_by_document_regex(match_all)
                .await
                .unwrap(),
            SignedRoaringBitmap::full()
        );
        assert_eq!(
            compact_metadata_provider
                .filter_by_document_regex(match_all)
                .await
                .unwrap(),
            SignedRoaringBitmap::full()
        );

        let selective_match = r"cat|dog";
        assert!(matches!(
            log_metadata_provider
                .filter_by_document_regex(selective_match)
                .await
                .unwrap(),
            SignedRoaringBitmap::Include(_)
        ),);
        assert!(matches!(
            compact_metadata_provider
                .filter_by_document_regex(selective_match)
                .await
                .unwrap(),
            SignedRoaringBitmap::Include(_)
        ),);
    }
}
