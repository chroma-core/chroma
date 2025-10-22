use crate::spann_provider::SpannProvider;

use super::{
    blockfile_metadata::MetadataSegmentWriter, blockfile_record::RecordSegmentWriter,
    distributed_hnsw::DistributedHNSWSegmentWriter, types::materialize_logs,
};
use chroma_blockstore::{provider::BlockfileProvider, test_arrow_blockfile_provider};
use chroma_config::registry::Registry;
use chroma_distance::{normalize, DistanceFunction};
use chroma_error::ChromaError;
use chroma_index::{
    hnsw_provider::HnswIndexProvider,
    spann::types::{GarbageCollectionContext, SpannMetrics},
    test_hnsw_index_provider,
};
use chroma_types::{
    operator::{
        CountResult, GetResult, KnnBatchResult, KnnProjectionOutput, KnnProjectionRecord,
        Projection, ProjectionOutput, ProjectionRecord,
    },
    plan::{Count, Get, Knn},
    test_segment, BooleanOperator, Chunk, Collection, CollectionAndSegments, CompositeExpression,
    DocumentExpression, DocumentOperator, KnnIndex, LogRecord, Metadata, MetadataComparison,
    MetadataExpression, MetadataSetValue, MetadataValue, Operation, OperationRecord,
    PrimitiveOperator, Schema, Segment, SegmentScope, SegmentUuid, SetOperator, UpdateMetadata,
    Where, CHROMA_KEY,
};
use regex::Regex;
use std::collections::BinaryHeap;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    ops::{BitAnd, BitOr},
    sync::atomic::AtomicU32,
};
use tempfile::TempDir;
use thiserror::Error;

pub struct TestDistributedSegment {
    pub temp_dirs: Vec<TempDir>,
    pub blockfile_provider: BlockfileProvider,
    pub hnsw_provider: HnswIndexProvider,
    pub spann_provider: SpannProvider,
    pub collection: Collection,
    pub metadata_segment: Segment,
    pub record_segment: Segment,
    pub vector_segment: Segment,
}

impl TestDistributedSegment {
    pub async fn new_with_dimension(dimension: usize) -> Self {
        let mut collection = Collection::test_collection(dimension as i32);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));
        let collection_uuid = collection.collection_id;
        let (blockfile_dir, blockfile_provider) = test_arrow_blockfile_provider(2 << 22);
        let (hnsw_dir, hnsw_provider) = test_hnsw_index_provider();
        let garbage_collection_context = GarbageCollectionContext::new(Registry::new())
            .await
            .expect("Expected to construct gc context for spann");

        Self {
            temp_dirs: vec![blockfile_dir, hnsw_dir],
            blockfile_provider: blockfile_provider.clone(),
            hnsw_provider: hnsw_provider.clone(),
            spann_provider: SpannProvider {
                hnsw_provider,
                blockfile_provider,
                garbage_collection_context,
                metrics: SpannMetrics::default(),
                pl_block_size: 5 * 1024 * 1024,
                adaptive_search_nprobe: true,
            },
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

        let mut metadata_writer = MetadataSegmentWriter::from_segment(
            &self.collection.tenant,
            &self.collection.database_id,
            &self.metadata_segment,
            &self.blockfile_provider,
        )
        .await
        .expect("Should be able to initialize metadata writer.");
        metadata_writer
            .apply_materialized_log_chunk(&None, &materialized_logs, None)
            .await
            .expect("Should be able to apply materialized logs.");
        metadata_writer
            .finish()
            .await
            .expect("Should be able to write to blockfile.");
        self.metadata_segment.file_path = Box::pin(
            Box::pin(metadata_writer.commit())
                .await
                .expect("Should be able to commit metadata.")
                .flush(),
        )
        .await
        .expect("Should be able to flush metadata.");

        let record_writer = RecordSegmentWriter::from_segment(
            &self.collection.tenant,
            &self.collection.database_id,
            &self.record_segment,
            &self.blockfile_provider,
        )
        .await
        .expect("Should be able to initiaize record writer.");
        record_writer
            .apply_materialized_log_chunk(&None, &materialized_logs)
            .await
            .expect("Should be able to apply materialized log.");

        self.record_segment.file_path = Box::pin(
            Box::pin(record_writer.commit())
                .await
                .expect("Should be able to commit record.")
                .flush(),
        )
        .await
        .expect("Should be able to flush record.");

        let vector_writer = DistributedHNSWSegmentWriter::from_segment(
            &self.collection,
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

impl From<&TestDistributedSegment> for CollectionAndSegments {
    fn from(value: &TestDistributedSegment) -> Self {
        Self {
            collection: value.collection.clone(),
            metadata_segment: value.metadata_segment.clone(),
            record_segment: value.record_segment.clone(),
            vector_segment: value.vector_segment.clone(),
        }
    }
}

impl TestDistributedSegment {
    pub async fn new() -> Self {
        Self::new_with_dimension(128).await
    }
}

#[derive(Debug, Error)]
pub enum TestReferenceSegmentError {
    #[error("Not found")]
    NotFound,
}

impl ChromaError for TestReferenceSegmentError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            TestReferenceSegmentError::NotFound => chroma_error::ErrorCodes::NotFound,
        }
    }
}

#[derive(Default, Debug, Clone)]
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

    fn filter_metadata(metadata: Option<UpdateMetadata>) -> Option<UpdateMetadata> {
        metadata.and_then(|metadata| {
            let filtered: UpdateMetadata = metadata
                .into_iter()
                .filter(|(k, _)| !k.starts_with(CHROMA_KEY))
                .collect();
            if filtered.is_empty() {
                None
            } else {
                Some(filtered)
            }
        })
    }

    pub fn create_segment(&mut self, segment: Segment) {
        self.record.insert(segment.id, HashMap::new());
    }

    pub fn apply_logs(&mut self, logs: Vec<LogRecord>, segment_id: SegmentUuid) {
        self.apply_operation_records(logs.into_iter().map(|l| l.record).collect(), segment_id);
    }

    pub fn apply_operation_records(
        &mut self,
        operations: Vec<OperationRecord>,
        segment_id: SegmentUuid,
    ) {
        let coll = self.record.entry(segment_id).or_default();
        for OperationRecord {
            id,
            embedding,
            encoding: _,
            metadata,
            document,
            operation,
        } in operations
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
                        record.metadata = Self::merge_meta(None, Self::filter_metadata(metadata));
                        entry.insert((self.max_id, record));
                        self.max_id += 1;
                    }
                }
                Operation::Update => {
                    if let Some((_, old_record)) = coll.get_mut(&id) {
                        if record.document.is_some() {
                            old_record.document = record.document;
                        }

                        if record.embedding.is_some() {
                            old_record.embedding = record.embedding;
                        }

                        old_record.metadata = Self::merge_meta(
                            old_record.metadata.clone(),
                            Self::filter_metadata(metadata),
                        );
                    }
                }
                Operation::Upsert => {
                    if let Some((_, old_record)) = coll.get_mut(&id) {
                        if record.document.is_some() {
                            old_record.document = record.document;
                        }

                        old_record.embedding = record.embedding;

                        old_record.metadata = Self::merge_meta(
                            old_record.metadata.clone(),
                            Self::filter_metadata(metadata),
                        );
                    } else {
                        record.metadata = Self::merge_meta(None, Self::filter_metadata(metadata));
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
        Ok(CountResult {
            count: coll.len() as u32,
            pulled_log_bytes: 0,
        })
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
                    .is_none_or(|ids| ids.contains(k))
                    && plan
                        .filter
                        .where_clause
                        .as_ref()
                        .is_none_or(|w| w.eval(rec))
            })
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>();

        records.sort_by_key(|(oid, _)| *oid);

        Ok(GetResult {
            pulled_log_bytes: 0,
            result: ProjectionOutput {
                records: records
                    .into_iter()
                    .skip(plan.limit.offset as usize)
                    .take(plan.limit.limit.unwrap_or(u32::MAX) as usize)
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
            },
        })
    }

    pub fn knn(
        &self,
        plan: Knn,
        distance_function: DistanceFunction,
    ) -> Result<KnnBatchResult, Box<dyn ChromaError>> {
        let coll = self
            .record
            .get(&plan.scan.collection_and_segments.metadata_segment.id)
            .ok_or(TestReferenceSegmentError::NotFound)
            .map_err(|e| e.boxed())?;

        let filtered_records = coll
            .iter()
            .filter(|(k, (_, rec))| {
                plan.filter
                    .query_ids
                    .as_ref()
                    .is_none_or(|ids| ids.contains(k))
                    && plan
                        .filter
                        .where_clause
                        .as_ref()
                        .is_none_or(|w| w.eval(rec))
            })
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>();

        struct RecordWithDistance(f32, ProjectionRecord);
        impl PartialEq for RecordWithDistance {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
        impl Eq for RecordWithDistance {}
        impl PartialOrd for RecordWithDistance {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for RecordWithDistance {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.partial_cmp(&other.0).unwrap()
            }
        }

        let mut result = KnnBatchResult::default();

        for embedding in plan.knn.embeddings {
            let mut max_heap: BinaryHeap<RecordWithDistance> =
                BinaryHeap::with_capacity(plan.knn.fetch as usize * 100);
            let target_vector = normalize(&embedding);

            for (_, record) in &filtered_records {
                let distance = match &distance_function {
                    DistanceFunction::Cosine => distance_function.distance(
                        &target_vector,
                        &normalize(record.embedding.as_ref().unwrap()),
                    ),
                    other => other.distance(&embedding, record.embedding.as_ref().unwrap()),
                };

                if max_heap.len() < plan.knn.fetch as usize {
                    max_heap.push(RecordWithDistance(distance, record.clone()));
                } else if distance < max_heap.peek().unwrap().0 {
                    max_heap.pop();
                    max_heap.push(RecordWithDistance(distance, record.clone()));
                }
            }

            result.results.push(KnnProjectionOutput {
                records: max_heap
                    .into_sorted_vec()
                    .into_iter()
                    .map(|RecordWithDistance(distance, record)| KnnProjectionRecord {
                        distance: Some(distance),
                        record,
                    })
                    .collect(),
            });
        }

        Ok(result)
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
        let document = record.document.as_ref();
        match self.operator {
            DocumentOperator::Contains => document.is_some_and(|doc| doc.contains(&self.pattern)),
            DocumentOperator::NotContains => {
                !document.is_some_and(|doc| doc.contains(&self.pattern))
            }
            DocumentOperator::Regex => {
                document.is_some_and(|doc| Regex::new(&self.pattern).unwrap().is_match(doc))
            }
            DocumentOperator::NotRegex => {
                !document.is_some_and(|doc| Regex::new(&self.pattern).unwrap().is_match(doc))
            }
        }
    }
}

impl CheckRecord for MetadataExpression {
    fn eval(&self, record: &ProjectionRecord) -> bool {
        // TODO: Allow mixed usage of int and float?
        let stored = record.metadata.as_ref().and_then(|m| m.get(&self.key));
        match &self.comparison {
            MetadataComparison::Primitive(primitive_operator, metadata_value) => {
                // Convert int to float to make comparisons easier
                let metadata_value = match metadata_value {
                    MetadataValue::Int(i) => {
                        if matches!(stored, Some(MetadataValue::Float(_))) {
                            MetadataValue::Float(*i as f64)
                        } else {
                            metadata_value.clone()
                        }
                    }
                    MetadataValue::Float(f) => {
                        if matches!(stored, Some(MetadataValue::Int(_))) {
                            MetadataValue::Int(*f as i64)
                        } else {
                            metadata_value.clone()
                        }
                    }
                    v => v.clone(),
                };

                let match_type = matches!(
                    (&stored, &metadata_value),
                    (Some(MetadataValue::Bool(_)), MetadataValue::Bool(_))
                        | (Some(MetadataValue::Int(_)), MetadataValue::Int(_))
                        | (Some(MetadataValue::Float(_)), MetadataValue::Float(_))
                        | (Some(MetadataValue::Str(_)), MetadataValue::Str(_))
                );
                match primitive_operator {
                    PrimitiveOperator::Equal => {
                        match_type && stored.is_some_and(|v| *v == metadata_value)
                    }
                    PrimitiveOperator::NotEqual => {
                        !match_type || stored.is_some_and(|v| *v != metadata_value)
                    }
                    PrimitiveOperator::GreaterThan => {
                        match_type && stored.is_some_and(|v| *v > metadata_value)
                    }
                    PrimitiveOperator::GreaterThanOrEqual => {
                        match_type && stored.is_some_and(|v| *v >= metadata_value)
                    }
                    PrimitiveOperator::LessThan => {
                        match_type && stored.is_some_and(|v| *v < metadata_value)
                    }
                    PrimitiveOperator::LessThanOrEqual => {
                        match_type && stored.is_some_and(|v| *v <= metadata_value)
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
