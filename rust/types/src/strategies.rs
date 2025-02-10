use crate::{
    Collection, CollectionAndSegments, CollectionUuid, DocumentExpression, DocumentOperator,
    LogRecord, MetadataExpression, MetadataValue, Operation, OperationRecord, PrimitiveOperator,
    ScalarEncoding, Segment, SegmentType, SegmentUuid, UpdateMetadata, UpdateMetadataValue, Where,
};
use proptest::{collection, prelude::*};
use serde_json::Value;

/**
 * Strategy for metadata.
 */
fn arbitrary_update_metadata(
    min_pairs: usize,
    max_pairs: usize,
) -> impl Strategy<Value = UpdateMetadata> {
    let num_pairs = (min_pairs..=max_pairs).boxed();

    num_pairs
        .clone()
        .prop_flat_map(|num_pairs| {
            let keys = proptest::collection::vec(proptest::arbitrary::any::<String>(), num_pairs);

            let values = proptest::collection::vec(
                prop_oneof![
                    proptest::strategy::Just(UpdateMetadataValue::None),
                    proptest::bool::ANY.prop_map(UpdateMetadataValue::Bool),
                    proptest::arbitrary::any::<i64>().prop_map(UpdateMetadataValue::Int),
                    (-1e6..1e6f64).prop_map(UpdateMetadataValue::Float),
                    proptest::arbitrary::any::<String>()
                        .prop_map(|v| { UpdateMetadataValue::Str(v) }),
                ],
                num_pairs,
            );

            (keys, values)
        })
        .prop_map(|(keys, values)| keys.into_iter().zip(values).collect::<UpdateMetadata>())
}

/**
 * Strategy for operation record.
 */
pub struct OperationRecordStrategyParams {
    pub min_embedding_size: usize,
    pub max_embedding_size: usize,
    pub min_metadata_pairs: usize,
    pub max_metadata_pairs: usize,
}

impl Default for OperationRecordStrategyParams {
    fn default() -> Self {
        Self {
            min_embedding_size: 3,
            max_embedding_size: 1024,
            min_metadata_pairs: 0,
            max_metadata_pairs: 10,
        }
    }
}

impl Arbitrary for OperationRecord {
    type Parameters = OperationRecordStrategyParams;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        let id = proptest::arbitrary::any::<String>();
        let embedding = proptest::collection::vec(
            proptest::arbitrary::any::<f32>(),
            args.min_embedding_size..=args.max_embedding_size,
        );
        let metadata = proptest::option::of(arbitrary_update_metadata(
            args.min_metadata_pairs,
            args.max_metadata_pairs,
        ));
        let document = proptest::option::of(proptest::arbitrary::any::<String>());
        let operation = prop_oneof![
            proptest::strategy::Just(Operation::Add),
            proptest::strategy::Just(Operation::Delete),
            proptest::strategy::Just(Operation::Update),
            proptest::strategy::Just(Operation::Upsert)
        ];

        (
            id,
            embedding,
            metadata,
            document,
            operation,
            proptest::bool::ANY,
        )
            .prop_map(
                |(id, embedding, metadata, document, operation, discard_embedding)| {
                    let embedding = match operation {
                        Operation::Add => Some(embedding),
                        Operation::Upsert => Some(embedding),
                        Operation::Update => {
                            if discard_embedding {
                                None
                            } else {
                                Some(embedding)
                            }
                        }
                        Operation::Delete => None,
                    };
                    let encoding = embedding.as_ref().map(|_| ScalarEncoding::FLOAT32);

                    OperationRecord {
                        id,
                        embedding,
                        metadata,
                        document,
                        operation,
                        encoding,
                    }
                },
            )
            .boxed()
    }
}

/// This will generate `4 * collection_max_size` log records for `collection_max_size` elements
pub struct TestCollectionDataParams {
    pub collection_max_size: usize,
}

impl Default for TestCollectionDataParams {
    fn default() -> Self {
        Self {
            collection_max_size: 100,
        }
    }
}

const PROP_TENANT: &str = "tenant_proptest";
const PROP_DB: &str = "database_proptest";
const PROP_COLL: &str = "collection_proptest";

#[derive(Debug)]
pub struct TestCollectionData {
    pub collection_and_segments: CollectionAndSegments,
    pub logs: Vec<LogRecord>,
}

impl Arbitrary for TestCollectionData {
    type Parameters = TestCollectionDataParams;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        let records = collection::vec(
            (any::<String>(), any::<[f32; 3]>()),
            args.collection_max_size,
        )
        .prop_map(|ids| {
            ids.into_iter()
                .flat_map(|(id, emb)| {
                    [
                        (
                            id.clone(),
                            Some(emb.into_iter().collect::<Vec<_>>()),
                            Operation::Add,
                        ),
                        (id.clone(), None, Operation::Update),
                        (
                            id.clone(),
                            Some(emb.into_iter().collect::<Vec<_>>()),
                            Operation::Upsert,
                        ),
                        (id.clone(), None, Operation::Delete),
                    ]
                })
                .collect::<Vec<_>>()
        })
        .prop_shuffle()
        .prop_map(|id_ops| {
            id_ops
                .into_iter()
                .enumerate()
                .map(|(log_offset, (id, embedding, operation))| LogRecord {
                    log_offset: log_offset as i64,
                    record: OperationRecord {
                        id: id.clone(),
                        embedding,
                        encoding: None,
                        metadata: (!matches!(operation, Operation::Delete)).then_some(
                            [
                                ("id".to_string(), UpdateMetadataValue::Str(id.clone())),
                                (
                                    "log_offset".to_string(),
                                    UpdateMetadataValue::Int(log_offset as i64),
                                ),
                                (
                                    "modulo_7".to_string(),
                                    UpdateMetadataValue::Int(log_offset as i64 % 7),
                                ),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                        document: (!matches!(operation, Operation::Delete))
                            .then_some(format!("<{id}>-<{log_offset}>")),
                        operation,
                    },
                })
                .collect::<Vec<_>>()
        });

        records
            .prop_map(move |logs| {
                let collection_id = CollectionUuid::new();
                let collection_and_segments = CollectionAndSegments {
                    collection: Collection {
                        collection_id,
                        name: PROP_COLL.to_string(),
                        configuration_json: Value::Null,
                        metadata: None,
                        dimension: Some(3),
                        tenant: PROP_TENANT.to_string(),
                        database: PROP_DB.to_string(),
                        log_position: 0,
                        version: 0,
                        total_records_post_compaction: 0,
                    },
                    metadata_segment: Segment {
                        id: SegmentUuid::new(),
                        r#type: SegmentType::Sqlite,
                        scope: crate::SegmentScope::METADATA,
                        collection: collection_id,
                        metadata: None,
                        file_path: Default::default(),
                    },
                    record_segment: Segment {
                        id: SegmentUuid::new(),
                        r#type: SegmentType::Sqlite,
                        scope: crate::SegmentScope::METADATA,
                        collection: collection_id,
                        metadata: None,
                        file_path: Default::default(),
                    },
                    vector_segment: Segment {
                        id: SegmentUuid::new(),
                        r#type: SegmentType::HnswLocalMemory,
                        scope: crate::SegmentScope::VECTOR,
                        collection: collection_id,
                        metadata: None,
                        file_path: Default::default(),
                    },
                };
                TestCollectionData {
                    collection_and_segments,
                    logs,
                }
            })
            .boxed()
    }
}

#[derive(Debug)]
pub struct TestWhereFilterParams {
    pub depth: u32,
    pub branch: u32,
    pub leaf: u32,
}

impl Default for TestWhereFilterParams {
    fn default() -> Self {
        Self {
            depth: 4,
            branch: 4,
            leaf: 32,
        }
    }
}

#[derive(Debug)]
pub struct TestWhereFilter {
    pub clause: Where,
}

impl Arbitrary for TestWhereFilter {
    type Parameters = TestWhereFilterParams;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        let doc_expr = (0..30).prop_map(|roll| {
            let digit: i32 = roll % 10;
            let operator = match roll % 3 {
                0 => DocumentOperator::Contains,
                _ => DocumentOperator::NotContains,
            };
            Where::Document(DocumentExpression {
                operator,
                text: digit.to_string(),
            })
        });
        let meta_expr = (0..42).prop_map(|roll| {
            let val = MetadataValue::Int(roll as i64 % 7);
            let op = match roll % 6 {
                0 => PrimitiveOperator::Equal,
                1 => PrimitiveOperator::GreaterThan,
                2 => PrimitiveOperator::GreaterThanOrEqual,
                3 => PrimitiveOperator::LessThan,
                4 => PrimitiveOperator::LessThanOrEqual,
                _ => PrimitiveOperator::NotEqual,
            };
            Where::Metadata(MetadataExpression {
                key: "modulo_7".to_string(),
                comparison: crate::MetadataComparison::Primitive(op, val),
            })
        });
        let leaf = prop_oneof![doc_expr, meta_expr];
        let max_branch = args.branch as usize;
        leaf.prop_recursive(args.depth, args.leaf, args.branch, move |inner| {
            prop_oneof![
                collection::vec(inner.clone(), 0..max_branch).prop_map(Where::conjunction),
                collection::vec(inner, 0..max_branch).prop_map(Where::disjunction)
            ]
        })
        .prop_map(|clause| TestWhereFilter { clause })
        .boxed()
    }
}
