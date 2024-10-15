use crate::{
    arrow::{
        block::delta::{data_record::DataRecordStorage, BlockDelta, BlockStorage},
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
};
use arrow::{
    array::{
        Array, BinaryBuilder, FixedSizeListArray, FixedSizeListBuilder, Float32Array,
        Float32Builder, StringArray, StringBuilder, StructArray,
    },
    datatypes::{Field, Fields},
};
use arrow::{
    array::{ArrayRef, BinaryArray},
    util::bit_util,
};
use chroma_types::{chroma_proto::UpdateMetadata, DataRecord};
use prost::Message;
use std::sync::Arc;

pub struct ValueBuilderWrapper {
    id_builder: StringBuilder,
    embedding_builder: FixedSizeListBuilder<Float32Builder>,
    metadata_builder: BinaryBuilder,
    document_builder: StringBuilder,
}

impl ArrowWriteableValue for &DataRecord<'_> {
    type ReadableValue<'referred_data> = DataRecord<'referred_data>;
    type ValueBuilder = Option<ValueBuilderWrapper>;
    type PreparedValue = (String, Vec<f32>, Option<Vec<u8>>, Option<String>);

    fn offset_size(item_count: usize) -> usize {
        let id_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
        let metdata_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
        let document_offset = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);

        id_offset + metdata_offset + document_offset
    }

    fn validity_size(item_count: usize) -> usize {
        let validity_bytes = bit_util::round_upto_multiple_of_64(bit_util::ceil(item_count, 8));
        // Both document and metadata can be null
        validity_bytes * 2
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::DataRecord(builder) => builder.add(prefix, key, value),
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::DataRecord(builder) => builder.delete(prefix, key),
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::DataRecord(DataRecordStorage::new())
    }

    fn get_value_builder() -> Self::ValueBuilder {
        None
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        let id = value.id.to_string();
        let embedding = value.embedding.to_vec();

        let mut metadata_size = 0;
        let metadata = match &value.metadata {
            Some(metadata) => {
                let metadata_proto = Into::<UpdateMetadata>::into(metadata.clone());
                let metadata_as_bytes = metadata_proto.encode_to_vec();
                metadata_size = metadata_as_bytes.len();
                Some(metadata_as_bytes)
            }
            None => None,
        };

        let mut document_size = 0;
        let document = match value.document {
            Some(document) => {
                document_size = document.len();
                Some(document.to_string())
            }
            None => None,
        };

        (id, embedding, metadata, document)
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ValueBuilder) {
        let (id, embedding, metadata, document) = value;

        // Lazily init so we know the embedding dimension
        if builder.is_none() {
            builder.replace(ValueBuilderWrapper {
                id_builder: StringBuilder::new(),
                embedding_builder: FixedSizeListBuilder::new(
                    Float32Builder::new(),
                    embedding.len() as i32,
                ),
                metadata_builder: BinaryBuilder::new(),
                document_builder: StringBuilder::new(),
            });
        }
        let builder = builder.as_mut().unwrap();

        builder.id_builder.append_value(id);

        let embedding_arr = builder.embedding_builder.values();
        for entry in embedding {
            embedding_arr.append_value(entry);
        }
        builder.embedding_builder.append(true);

        builder.metadata_builder.append_option(metadata);
        builder.document_builder.append_option(document);
    }

    fn finish(mut builder: Self::ValueBuilder) -> (Field, Arc<dyn Array>) {
        let mut builder = builder.take().unwrap();

        let id_field = Field::new("id", arrow::datatypes::DataType::Utf8, true);
        let embedding_field = Field::new(
            "embedding",
            arrow::datatypes::DataType::FixedSizeList(
                Arc::new(Field::new(
                    "item",
                    arrow::datatypes::DataType::Float32,
                    true,
                )),
                builder.embedding_builder.value_length(),
            ),
            true,
        );
        let metadata_field = Field::new("metadata", arrow::datatypes::DataType::Binary, true);
        let document_field = Field::new("document", arrow::datatypes::DataType::Utf8, true);

        let id_arr = builder.id_builder.finish();
        let embedding_arr = builder.embedding_builder.finish();
        let metadata_arr = builder.metadata_builder.finish();
        let document_arr = builder.document_builder.finish();

        let struct_arr = StructArray::from(vec![
            (Arc::new(id_field.clone()), Arc::new(id_arr) as ArrayRef),
            (
                Arc::new(embedding_field.clone()),
                Arc::new(embedding_arr) as ArrayRef,
            ),
            (
                Arc::new(metadata_field.clone()),
                Arc::new(metadata_arr) as ArrayRef,
            ),
            (
                Arc::new(document_field.clone()),
                Arc::new(document_arr) as ArrayRef,
            ),
        ]);
        let struct_fields = Fields::from(vec![
            id_field,
            embedding_field,
            metadata_field,
            document_field,
        ]);
        let struct_field = Field::new(
            "value",
            arrow::datatypes::DataType::Struct(struct_fields),
            true,
        );
        let value_arr = (&struct_arr as &dyn Array).slice(0, struct_arr.len());

        (struct_field, value_arr)
    }
}

impl<'referred_data> ArrowReadableValue<'referred_data> for DataRecord<'referred_data> {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self {
        let as_struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

        // Read out id
        let id_arr = as_struct_array
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Read out embedding
        let embedding_arr = as_struct_array
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let target_vec = embedding_arr.value(index);
        let embedding_len = target_vec.len();
        let embedding_values = embedding_arr
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values();
        let embedding =
            &embedding_values[(index * embedding_len)..(index * embedding_len) + embedding_len];

        // Read out metadata
        let metadata_arr = as_struct_array
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let metadata_bytes = metadata_arr.value(index);
        let metadata = match metadata_bytes.len() {
            0 => None,
            _ => {
                let metadata_proto = UpdateMetadata::decode(metadata_bytes).unwrap();
                // TODO: unwrap error handling
                Some(metadata_proto.try_into().unwrap())
            }
        };

        // Read out document
        let document_arr = as_struct_array
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let document = match document_arr.is_null(index) {
            true => None,
            false => Some(document_arr.value(index)),
        };

        DataRecord {
            id: id_arr.value(index),
            embedding,
            metadata,
            document,
        }
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        delta: &mut BlockDelta,
    ) {
        delta.add(prefix, key, &value);
    }
}
