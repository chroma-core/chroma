use crate::{
    arrow::{
        block::delta::{data_record::DataRecordStorage, BlockDelta, BlockStorage},
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
};
use arrow::array::{Array, FixedSizeListArray, Float32Array, StringArray, StructArray};
use arrow::{array::BinaryArray, util::bit_util};
use chroma_types::{chroma_proto::UpdateMetadata, DataRecord};
use prost::Message;
use std::sync::Arc;

impl ArrowWriteableValue for &DataRecord<'_> {
    type ReadableValue<'referred_data> = DataRecord<'referred_data>;

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
