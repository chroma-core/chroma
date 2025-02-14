use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    ops::{BitAnd, BitOr},
    sync::atomic::AtomicU32,
};

use chroma_blockstore::{provider::BlockfileProvider, test_arrow_blockfile_provider};
use chroma_index::{hnsw_provider::HnswIndexProvider, test_hnsw_index_provider};
use chroma_types::{
    operator::{CountResult, GetResult, Projection, ProjectionOutput, ProjectionRecord},
    plan::{Count, Get},
    test_segment, BooleanOperator, Chunk, Collection, CollectionAndSegments, CompositeExpression,
    DocumentExpression, DocumentOperator, LogRecord, Metadata, MetadataComparison,
    MetadataExpression, MetadataSetValue, MetadataValue, Operation, OperationRecord,
    PrimitiveOperator, Segment, SegmentScope, SegmentUuid, SetOperator, UpdateMetadata, Where,
};
use thiserror::Error;

use super::{
    blockfile_metadata::MetadataSegmentWriter, blockfile_record::RecordSegmentWriter,
    distributed_hnsw::DistributedHNSWSegmentWriter, types::materialize_logs,
};

#[derive(Clone)]
pub struct TestDistributedSegment {
    pub blockfile_provider: BlockfileProvider,
    pub hnsw_provider: HnswIndexProvider,
    pub collection: Collection,
    pub metadata_segment: Segment,
    pub record_segment: Segment,
    pub vector_segment: Segment,
}

impl TestDistributedSegment {
    pub fn new_with_dimension(dimension: usize) -> Self {
        let collection = Collection::test_collection(dimension as i32);
        let collection_uuid = collection.collection_id;
        Self {
            blockfile_provider: test_arrow_blockfile_provider(2 << 22),
            hnsw_provider: test_hnsw_index_provider(),
            collection,
            metadata_segment: test_segment(collection_uuid, SegmentScope::METADATA),
            record_segment: test_segment(collection_uuid, SegmentScope::RECORD),
            vector_segment: test_segment(collection_uuid, SegmentScope::VECTOR),
        }
    }

    // WARN: The size of the log chunk should not be too large
    pub async fn compact_log(&mut self, logs: Chunk<LogRecord>, next_offset: usize) {
        let materialized_logs =
            materialize_logs(&None, logs, Some(AtomicU32::new(next_offset as u32).into()))
                .await
                .expect("Should be able to materialize log.");

        let mut metadata_writer =
            MetadataSegmentWriter::from_segment(&self.metadata_segment, &self.blockfile_provider)
                .await
                .expect("Should be able to initialize metadata writer.");
        metadata_writer
            .apply_materialized_log_chunk(&None, &materialized_logs)
            .await
            .expect("Should be able to apply materialized logs.");
        metadata_writer
            .finish()
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
            .apply_materialized_log_chunk(&None, &materialized_logs)
            .await
            .expect("Should be able to apply materialized log.");

        self.record_segment.file_path = record_writer
            .commit()
            .await
            .expect("Should be able to commit record.")
            .flush()
            .await
            .expect("Should be able to flush record.");

        let vector_writer = DistributedHNSWSegmentWriter::from_segment(
            &self.vector_segment,
            self.collection
                .dimension
                .expect("Collection dimension should be set") as usize,
            self.hnsw_provider.clone(),
        )
        .await
        .expect("Should be able to initialize vector writer");

        vector_writer
            .apply_materialized_log_chunk(&None, &materialized_logs)
            .await
            .expect("Should be able to apply materialized log.");

        self.vector_segment.file_path = vector_writer
            .commit()
            .await
            .expect("Should be able to commit vector.")
            .flush()
            .await
            .expect("Should be able to flush vector.");
    }
}

impl From<TestDistributedSegment> for CollectionAndSegments {
    fn from(value: TestDistributedSegment) -> Self {
        Self {
            collection: value.collection,
            metadata_segment: value.metadata_segment,
            record_segment: value.record_segment,
            vector_segment: value.vector_segment,
        }
    }
}

impl Default for TestDistributedSegment {
    fn default() -> Self {
        Self::new_with_dimension(128)
    }
}

#[derive(Debug, Error)]
pub enum TestReferenceSegmentError {
    #[error("Not found")]
    NotFound,
}

#[derive(Default)]
pub struct TestReferenceSegment {
    max_id: u32,
    record: HashMap<SegmentUuid, HashMap<String, (u32, ProjectionRecord)>>,
}

impl TestReferenceSegment {
    fn merge_meta(old_meta: Option<Metadata>, delta: Option<UpdateMetadata>) -> Option<Metadata> {
        let (deleted_keys, new_meta) = if let Some(m) = delta {
            let mut dk = HashSet::new();
            let mut nm = HashMap::new();
            for (k, v) in m {
                match MetadataValue::try_from(&v) {
                    Ok(mv) => {
                        nm.insert(k, mv);
                    }
                    Err(_) => {
                        dk.insert(k);
                    }
                }
            }
            (dk, Some(nm))
        } else {
            (HashSet::new(), None)
        };
        match (old_meta, new_meta) {
            (None, None) => None,
            (None, Some(m)) | (Some(m), None) => Some(m),
            (Some(o), Some(n)) => Some(
                o.into_iter()
                    .filter(|(k, _)| !deleted_keys.contains(k))
                    .chain(n)
                    .collect(),
            ),
        }
    }

    pub fn apply_logs(&mut self, logs: Vec<LogRecord>, segmemt_id: SegmentUuid) {
        let coll = self.record.entry(segmemt_id).or_default();
        for LogRecord {
            log_offset: _,
            record:
                OperationRecord {
                    id,
                    embedding,
                    encoding: _,
                    metadata,
                    document,
                    operation,
                },
        } in logs
        {
            let mut record = ProjectionRecord {
                id: id.clone(),
                document,
                embedding,
                metadata: None,
            };
            match operation {
                Operation::Add => {
                    if let Entry::Vacant(entry) = coll.entry(id) {
                        record.metadata = Self::merge_meta(None, metadata);
                        entry.insert((self.max_id, record));
                        self.max_id += 1;
                    }
                }
                Operation::Update => {
                    if let Some((_, old_record)) = coll.get_mut(&id) {
                        old_record.document = record.document;
                        old_record.embedding = record.embedding;
                        old_record.metadata =
                            Self::merge_meta(old_record.metadata.clone(), metadata);
                    }
                }
                Operation::Upsert => {
                    if let Some((_, old_record)) = coll.get_mut(&id) {
                        old_record.document = record.document;
                        old_record.embedding = record.embedding;
                        old_record.metadata =
                            Self::merge_meta(old_record.metadata.clone(), metadata);
                    } else {
                        record.metadata = Self::merge_meta(None, metadata);
                        coll.insert(id, (self.max_id, record));
                        self.max_id += 1;
                    }
                }
                Operation::Delete => {
                    coll.remove(&id);
                }
            };
        }
    }

    pub fn count(&self, plan: Count) -> Result<CountResult, TestReferenceSegmentError> {
        let coll = self
            .record
            .get(&plan.scan.collection_and_segments.metadata_segment.id)
            .ok_or(TestReferenceSegmentError::NotFound)?;
        Ok(coll.len() as u32)
    }

    pub fn get(&self, plan: Get) -> Result<GetResult, TestReferenceSegmentError> {
        let coll = self
            .record
            .get(&plan.scan.collection_and_segments.metadata_segment.id)
            .ok_or(TestReferenceSegmentError::NotFound)?;
        let mut records = coll
            .iter()
            .filter(|(k, (_, rec))| {
                plan.filter
                    .query_ids
                    .as_ref()
                    .map_or(true, |ids| ids.contains(k))
                    && plan
                        .filter
                        .where_clause
                        .as_ref()
                        .map_or(true, |w| w.eval(rec))
            })
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>();

        records.sort_by_key(|(oid, _)| *oid);

        Ok(ProjectionOutput {
            records: records
                .into_iter()
                .skip(plan.limit.skip as usize)
                .take(plan.limit.fetch.unwrap_or(u32::MAX) as usize)
                .map(|(_, mut rec)| {
                    let Projection {
                        document,
                        embedding,
                        metadata,
                    } = plan.proj;
                    if !document {
                        rec.document = None;
                    }
                    if !embedding {
                        rec.embedding = None;
                    }
                    if !metadata || rec.metadata.as_ref().is_some_and(|meta| meta.is_empty()) {
                        rec.metadata = None;
                    }
                    rec
                })
                .collect(),
        })
    }
}

/// Given a record, verify if the predicate evaluates to true on it
/// This is intended to be used with the reference segment impl
/// This should be implemented for all (sub)types of the where clause
trait CheckRecord {
    fn eval(&self, record: &ProjectionRecord) -> bool;
}

impl CheckRecord for Where {
    fn eval(&self, record: &ProjectionRecord) -> bool {
        match self {
            Where::Composite(composite_expression) => composite_expression.eval(record),
            Where::Document(document_expression) => document_expression.eval(record),
            Where::Metadata(metadata_expression) => metadata_expression.eval(record),
        }
    }
}

impl CheckRecord for CompositeExpression {
    fn eval(&self, record: &ProjectionRecord) -> bool {
        let children_evals = self.children.iter().map(|child| child.eval(record));
        match self.operator {
            BooleanOperator::And => children_evals.fold(true, BitAnd::bitand),
            BooleanOperator::Or => children_evals.fold(false, BitOr::bitor),
        }
    }
}

impl CheckRecord for DocumentExpression {
    fn eval(&self, record: &ProjectionRecord) -> bool {
        let contains = record
            .document
            .as_ref()
            .is_some_and(|doc| doc.contains(&self.text));
        match self.operator {
            DocumentOperator::Contains => contains,
            DocumentOperator::NotContains => !contains,
        }
    }
}

impl CheckRecord for MetadataExpression {
    fn eval(&self, record: &ProjectionRecord) -> bool {
        // TODO: Allow mixed usage of int and float?
        let stored = record.metadata.as_ref().and_then(|m| m.get(&self.key));
        match &self.comparison {
            MetadataComparison::Primitive(primitive_operator, metadata_value) => {
                let match_type = matches!(
                    (stored, metadata_value),
                    (Some(MetadataValue::Bool(_)), MetadataValue::Bool(_))
                        | (Some(MetadataValue::Int(_)), MetadataValue::Int(_))
                        | (Some(MetadataValue::Float(_)), MetadataValue::Float(_))
                        | (Some(MetadataValue::Str(_)), MetadataValue::Str(_))
                );
                match primitive_operator {
                    PrimitiveOperator::Equal => {
                        match_type && stored.is_some_and(|v| v == metadata_value)
                    }
                    PrimitiveOperator::NotEqual => {
                        !match_type || stored.is_some_and(|v| v != metadata_value)
                    }
                    PrimitiveOperator::GreaterThan => {
                        match_type && stored.is_some_and(|v| v > metadata_value)
                    }
                    PrimitiveOperator::GreaterThanOrEqual => {
                        match_type && stored.is_some_and(|v| v >= metadata_value)
                    }
                    PrimitiveOperator::LessThan => {
                        match_type && stored.is_some_and(|v| v < metadata_value)
                    }
                    PrimitiveOperator::LessThanOrEqual => {
                        match_type && stored.is_some_and(|v| v <= metadata_value)
                    }
                }
            }
            MetadataComparison::Set(set_operator, metadata_set_value) => {
                let contains = match (stored, metadata_set_value) {
                    (Some(MetadataValue::Bool(val)), MetadataSetValue::Bool(vec)) => {
                        vec.contains(val)
                    }
                    (Some(MetadataValue::Int(val)), MetadataSetValue::Int(vec)) => {
                        vec.contains(val)
                    }
                    (Some(MetadataValue::Float(val)), MetadataSetValue::Float(vec)) => {
                        vec.contains(val)
                    }
                    (Some(MetadataValue::Str(val)), MetadataSetValue::Str(vec)) => {
                        vec.contains(val)
                    }
                    _ => false,
                };
                match set_operator {
                    SetOperator::In => contains,
                    SetOperator::NotIn => !contains,
                }
            }
        }
    }
}
