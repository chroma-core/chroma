use crate::{
    arrow::{
        block::delta::{string::StringValueStorage, BlockDelta, BlockStorage},
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::{CompositeKey, KeyWrapper},
};
use arrow::{
    array::{Array, StringArray},
    util::bit_util,
};
use std::sync::Arc;

impl ArrowWriteableValue for &str {
    type ReadableValue<'referred_data> = &'referred_data str;

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
    }

    fn validity_size(item_count: usize) -> usize {
        0 // We don't support None values for StringArray
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::String(builder) => {
                let mut storage = builder.storage.write();
                match storage.as_mut() {
                    Some(storage) => {
                        storage.insert(
                            CompositeKey {
                                prefix: prefix.to_string(),
                                key,
                            },
                            value.to_string(),
                        );
                    }
                    None => {
                        unreachable!("Storage not initialized. This is an invariant violation.")
                    }
                }
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::String(builder) => {
                let mut storage = builder.storage.write();
                match storage.as_mut() {
                    Some(storage) => {
                        storage.remove(&CompositeKey {
                            prefix: prefix.to_string(),
                            key,
                        });
                    }
                    None => {
                        unreachable!("Storage not initialized. This is an invariant violation.")
                    }
                }
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::String(StringValueStorage::new())
    }
}

impl<'referred_data> ArrowReadableValue<'referred_data> for &'referred_data str {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> &'referred_data str {
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        array.value(index)
    }
    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        delta: &mut BlockDelta,
    ) {
        delta.add(prefix, key, value);
    }
}
