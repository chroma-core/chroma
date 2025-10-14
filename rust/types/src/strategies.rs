use crate::{
    regex::hir::ChromaHir, Collection, CollectionAndSegments, CollectionUuid, DocumentExpression,
    DocumentOperator, IncludeList, LogRecord, Metadata, MetadataComparison, MetadataExpression,
    MetadataSetValue, MetadataValue, Operation, OperationRecord, PrimitiveOperator, ScalarEncoding,
    Segment, SegmentType, SegmentUuid, SetOperator, UpdateMetadata, UpdateMetadataValue, Where,
};
use proptest::{collection, prelude::*, sample::SizeRange, string::string_regex};
use regex_syntax::hir::{ClassUnicode, ClassUnicodeRange};

/**
 * Strategy for valid metadata keys.
 * Keys cannot be empty and cannot start with '#' or '$'.
 */
fn valid_metadata_key() -> impl Strategy<Value = String> {
    // Regex: at least one character, first character cannot be # or $
    string_regex("[^#$].{0,99}").unwrap()
}

/**
 * Strategy for metadata.
 */
pub fn arbitrary_update_metadata(
    num_pairs: impl Into<SizeRange>,
) -> impl Strategy<Value = UpdateMetadata> {
    proptest::collection::hash_map(
        valid_metadata_key(),
        proptest::arbitrary::any::<UpdateMetadataValue>(),
        num_pairs,
    )
}

pub fn arbitrary_metadata(num_pairs: impl Into<SizeRange>) -> impl Strategy<Value = Metadata> {
    proptest::collection::hash_map(
        valid_metadata_key(),
        proptest::arbitrary::any::<MetadataValue>(),
        num_pairs,
    )
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
            args.min_metadata_pairs..=args.max_metadata_pairs,
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

#[derive(Debug, Clone)]
pub struct TestCollectionData {
    pub collection_and_segments: CollectionAndSegments,
    pub logs: Vec<LogRecord>,
}

impl Arbitrary for TestCollectionData {
    type Parameters = TestCollectionDataParams;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        let records = collection::vec(("\\PC{1,}", any::<[f32; 3]>()), args.collection_max_size)
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
                        dimension: Some(3),
                        tenant: PROP_TENANT.to_string(),
                        database: PROP_DB.to_string(),
                        ..Default::default()
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
    pub seed_documents: Option<Vec<String>>,
    pub seed_metadata: Option<Vec<Metadata>>,
}

impl Default for TestWhereFilterParams {
    fn default() -> Self {
        Self {
            depth: 4,
            branch: 4,
            leaf: 32,
            seed_documents: None,
            seed_metadata: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TestWhereFilter {
    pub clause: Where,
}

const MIN_DOCUMENT_FILTER_LENGTH: usize = 3;
pub const DOCUMENT_TEXT_STRATEGY: &str = "\\PC{3,}";

impl Arbitrary for TestWhereFilter {
    type Parameters = TestWhereFilterParams;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        let doc_string = if let Some(seed_documents) = args.seed_documents {
            if seed_documents.is_empty() {
                DOCUMENT_TEXT_STRATEGY.boxed()
            } else {
                prop_oneof![
                    1 => DOCUMENT_TEXT_STRATEGY,
                    3 => any::<proptest::sample::Index>()
                        .prop_map(move |index| index.get(&seed_documents).clone())
                        .prop_flat_map(move |s| {
                            let len = s.char_indices().count();
                            (
                                Just(s),
                                0..=(len - MIN_DOCUMENT_FILTER_LENGTH),
                                MIN_DOCUMENT_FILTER_LENGTH..=len,
                            )
                        })
                        .prop_map(|(s, start, len)| {
                            let start = s.char_indices().nth(start).map_or(0, |(i, _)| i);
                            let end = s
                                .char_indices()
                                .nth(start + len)
                                .map_or(s.len(), |(i, _)| i);
                            s[start..end].to_string()
                        }),
                ]
                .boxed()
            }
        } else {
            DOCUMENT_TEXT_STRATEGY.boxed()
        };

        let doc_operator = prop_oneof![
            proptest::strategy::Just(DocumentOperator::Contains),
            proptest::strategy::Just(DocumentOperator::NotContains),
        ];
        let document_expression_strategy =
            (doc_string, doc_operator).prop_map(|(text, operator)| {
                Where::Document(DocumentExpression {
                    operator,
                    pattern: text.to_string(),
                })
            });

        let metadata_pair_strategy = if let Some(seed_metadata) = &args.seed_metadata {
            let mut metadata_pairs = seed_metadata
                .clone()
                .into_iter()
                .flat_map(|m| m.into_iter())
                .collect::<Vec<_>>();
            metadata_pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));

            if !metadata_pairs.is_empty() {
                let seeded_metadata_strategy = any::<proptest::sample::Index>()
                    .prop_map(move |index| index.get(&metadata_pairs).clone());

                prop_oneof![
                    1 => ("\\PC", any::<MetadataValue>()),
                    1 => (seeded_metadata_strategy.clone().prop_map(|(k, _v)| k), any::<MetadataValue>()),
                    1 => ("\\PC", seeded_metadata_strategy.clone().prop_map(|(_k, v)| v)),
                    5 => seeded_metadata_strategy,
                ]
                .boxed()
            } else {
                ("\\PC", any::<MetadataValue>()).boxed()
            }
        } else {
            ("\\PC", any::<MetadataValue>()).boxed()
        };

        let metadata_expression_strategy = metadata_pair_strategy.prop_flat_map(|(key, value)| {
            prop_oneof![
                any::<PrimitiveOperator>().prop_map({
                    let key = key.clone();
                    let value = value.clone();

                    move |op| {
                        Where::Metadata(MetadataExpression {
                            key: key.clone(),
                            comparison: MetadataComparison::Primitive(op, value.clone()),
                        })
                    }
                }),
                any::<SetOperator>().prop_map(move |op| {
                    Where::Metadata(MetadataExpression {
                        key: key.to_string(),
                        comparison: MetadataComparison::Set(
                            op,
                            match value.clone() {
                                MetadataValue::Bool(v) => MetadataSetValue::Bool(vec![v]),
                                MetadataValue::Int(v) => MetadataSetValue::Int(vec![v]),
                                MetadataValue::Float(v) => MetadataSetValue::Float(vec![v]),
                                MetadataValue::Str(v) => MetadataSetValue::Str(vec![v]),
                                MetadataValue::SparseVector(_) => {
                                    unreachable!("Metadata expression should not use sparse vector")
                                }
                            },
                        ),
                    })
                }),
            ]
        });

        let leaf = prop_oneof![metadata_expression_strategy, document_expression_strategy];
        let max_branch = args.branch as usize;
        let recursive_strategy = leaf
            .prop_recursive(args.depth, args.leaf, args.branch, move |inner| {
                prop_oneof![
                    collection::vec(inner.clone(), 0..max_branch).prop_map(Where::conjunction),
                    collection::vec(inner, 0..max_branch).prop_map(Where::disjunction)
                ]
            })
            .prop_map(|clause| TestWhereFilter { clause });

        recursive_strategy.boxed()
    }
}

impl Arbitrary for IncludeList {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        let all = IncludeList::all();
        let size = all.0.len();
        proptest::sample::subsequence(all.0, 0..=size)
            .prop_map(IncludeList)
            .boxed()
    }
}

/// Generates collection data and a where filter seeded with the collection data.
pub fn any_collection_data_and_where_filter(
) -> impl Strategy<Value = (TestCollectionData, TestWhereFilter)> {
    any::<TestCollectionData>().prop_flat_map(|data| {
        let seed_documents = data
            .logs
            .iter()
            .filter_map(|log| log.record.document.clone())
            .collect::<Vec<_>>();
        let seed_metadata = data
            .logs
            .iter()
            .filter_map(|log| {
                log.record.metadata.clone().map(|m| {
                    m.into_iter()
                        .filter_map(|(k, v)| {
                            let v: MetadataValue = (&v).try_into().ok()?;
                            Some((k, v))
                        })
                        .collect()
                })
            })
            .collect::<Vec<_>>();
        (
            Just(data),
            any_with::<TestWhereFilter>(TestWhereFilterParams {
                seed_documents: Some(seed_documents),
                seed_metadata: Some(seed_metadata),
                ..Default::default()
            }),
        )
    })
}

#[derive(Clone, Debug, Default)]
pub struct ArbitraryChromaHirParameters {
    pub recursive: bool,
}

impl Arbitrary for ChromaHir {
    type Parameters = ArbitraryChromaHirParameters;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        let literal = r"\w{3,}".prop_map(Self::Literal);
        let char_class = prop_oneof![
            2 => Just(Self::Class(ClassUnicode::new([
                ClassUnicodeRange::new('a', 'z'),
                ClassUnicodeRange::new('A', 'Z'),
                ClassUnicodeRange::new('0', '9'),
                ClassUnicodeRange::new('_', '_'),
            ]))),
            1 => r"[a-z]".prop_map(|mut word_char| {
                let wchr = word_char.pop().unwrap();
                Self::Class(ClassUnicode::new([
                    ClassUnicodeRange::new(wchr.to_ascii_lowercase(), wchr.to_ascii_lowercase()),
                    ClassUnicodeRange::new(wchr.to_ascii_uppercase(), wchr.to_ascii_uppercase()),
                ]))
            })
        ];
        let primitive = prop_oneof![
            2 => literal,
            1 => char_class,
        ];
        if args.recursive {
            primitive
                .prop_recursive(3, 12, 3, |inner| {
                    prop_oneof![
                        2 => collection::vec(inner.clone(), 2..4).prop_map(Self::Concat),
                        3 => collection::vec(inner.clone(), 2..4).prop_map(Self::Alternation),
                        1 => inner.prop_map(|hir| Self::Repetition {
                            min: 0,
                            max: None,
                            sub: Box::new(hir)
                        }),
                    ]
                })
                .boxed()
        } else {
            primitive.boxed()
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChromaRegexTestDocuments {
    pub documents: Vec<String>,
    pub hir: ChromaHir,
}

#[derive(Clone, Debug)]
pub struct ArbitraryChromaRegexTestDocumentsParameters {
    pub recursive_hir: bool,
    pub total_document_count: usize,
}

impl Default for ArbitraryChromaRegexTestDocumentsParameters {
    fn default() -> Self {
        Self {
            recursive_hir: true,
            total_document_count: 100,
        }
    }
}

impl Arbitrary for ChromaRegexTestDocuments {
    type Parameters = ArbitraryChromaRegexTestDocumentsParameters;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        ChromaHir::arbitrary_with(ArbitraryChromaHirParameters {
            recursive: args.recursive_hir,
        })
        .prop_flat_map(move |hir| {
            let doc_count = args.total_document_count;
            let pattern_str = String::from(hir.clone());
            collection::vec(
                prop_oneof![
                    string_regex(&pattern_str)
                        .unwrap()
                        .prop_map(|doc| if doc.len() < 3 {
                            format!("^|{doc}|$")
                        } else {
                            doc
                        }),
                    DOCUMENT_TEXT_STRATEGY
                ],
                doc_count..=doc_count,
            )
            .prop_map(move |documents| ChromaRegexTestDocuments {
                documents,
                hir: hir.clone(),
            })
        })
        .boxed()
    }
}
