use super::{
    delta::BlockDelta,
    delta_storage::{BlockStorage, DataRecordStorage},
};
use crate::{
    arrow::types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    key::{CompositeKey, KeyWrapper},
};
use arrow::array::BinaryArray;
use arrow::{
    array::{Array, FixedSizeListArray, Float32Array, StringArray, StructArray},
    util::bit_util,
};
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
        return validity_bytes * 2;
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::DataRecord(builder) => {
                let mut id_storage = builder.id_storage.write();
                let mut embedding_storage = builder.embedding_storage.write();
                let composite_key = CompositeKey {
                    prefix: prefix.to_string(),
                    key,
                };

                id_storage.insert(composite_key.clone(), value.id.to_string());
                embedding_storage.insert(composite_key.clone(), value.embedding.to_vec());

                match &value.metadata {
                    Some(metadata) => {
                        let mut metadata_storage = builder.metadata_storage.write();
                        let metadata_proto = Into::<UpdateMetadata>::into(metadata.clone());
                        metadata_storage
                            .insert(composite_key.clone(), Some(metadata_proto.encode_to_vec()));
                    }
                    None => {
                        let mut metadata_storage = builder.metadata_storage.write();
                        metadata_storage.insert(composite_key.clone(), None);
                    }
                }

                let mut document_storage = builder.document_storage.write();
                document_storage.insert(
                    composite_key,
                    value.document.map_or(None, |doc| Some(doc.to_string())),
                );
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::DataRecord(builder) => {
                let mut id_storage = builder.id_storage.write();
                let mut embedding_storage = builder.embedding_storage.write();
                let mut metadata_storage = builder.metadata_storage.write();
                let mut document_storage = builder.document_storage.write();
                id_storage.remove(&CompositeKey {
                    prefix: prefix.to_string(),
                    key: key.clone(),
                });
                embedding_storage.remove(&CompositeKey {
                    prefix: prefix.to_string(),
                    key: key.clone(),
                });
                metadata_storage.remove(&CompositeKey {
                    prefix: prefix.to_string(),
                    key: key.clone(),
                });
                document_storage.remove(&CompositeKey {
                    prefix: prefix.to_string(),
                    key,
                });
            }
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
            id: &id_arr.value(index),
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
