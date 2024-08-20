use crate::arrow::{
    block::delta::{roaring_bitmap::RoaringBitmapStorage, BlockDelta, BlockStorage},
    types::{ArrowReadableValue, ArrowWriteableValue},
};
use arrow::{array::BinaryArray, util::bit_util};
use roaring::RoaringBitmap;

impl ArrowWriteableValue for &RoaringBitmap {
    type ReadableValue<'referred_data> = RoaringBitmap;

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
    }

    fn validity_size(_item_count: usize) -> usize {
        0 // We don't support None values for RoaringBitmap
    }

    fn add(prefix: &str, key: crate::key::KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::RoaringBitmap(builder) => {
                let mut builder = builder.storage.write();
                let mut serialized = Vec::with_capacity(value.serialized_size());
                let res = value.serialize_into(&mut serialized);
                // TODO: proper error handling
                let serialized = match res {
                    Ok(_) => serialized,
                    Err(e) => panic!("Failed to serialize RoaringBitmap: {}", e),
                };
                builder.insert(
                    crate::key::CompositeKey {
                        prefix: prefix.to_string(),
                        key,
                    },
                    serialized,
                );
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: crate::key::KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::RoaringBitmap(builder) => {
                let mut builder = builder.storage.write();
                builder.remove(&crate::key::CompositeKey {
                    prefix: prefix.to_string(),
                    key,
                });
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::RoaringBitmap(RoaringBitmapStorage::new())
    }
}

impl ArrowReadableValue<'_> for RoaringBitmap {
    fn get(array: &std::sync::Arc<dyn arrow::array::Array>, index: usize) -> Self {
        let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        let bytes = arr.value(index);
        // TODO: proper error handling
        RoaringBitmap::deserialize_from(bytes).unwrap()
    }

    fn add_to_delta<K: crate::arrow::types::ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        delta: &mut BlockDelta,
    ) {
        delta.add(prefix, key, &value);
    }
}
