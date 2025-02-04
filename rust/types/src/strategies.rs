use crate::{Operation, OperationRecord, ScalarEncoding, UpdateMetadata, UpdateMetadataValue};
use proptest::prelude::*;

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
