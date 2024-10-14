use super::{data_record::DataRecordStorage, single_column_storage::SingleColumnStorage};
use crate::{
    arrow::types::ArrowWriteableKey,
    key::{CompositeKey, KeyWrapper},
};
use arrow::{
    array::{
        Array, ArrayRef, BooleanBuilder, Float32Builder, RecordBatch, StringBuilder, UInt32Builder,
    },
    datatypes::Field,
};
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    fmt::{self, Debug, Formatter},
};

#[derive(Clone)]
pub enum BlockStorage {
    String(SingleColumnStorage<String>),
    VecUInt32(SingleColumnStorage<Vec<u32>>),
    UInt32(SingleColumnStorage<u32>),
    RoaringBitmap(SingleColumnStorage<RoaringBitmap>),
    DataRecord(DataRecordStorage),
}

impl Debug for BlockStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BlockStorage::String(_) => f.debug_struct("String").finish(),
            BlockStorage::VecUInt32(_) => f.debug_struct("VecUInt32").finish(),
            BlockStorage::UInt32(_) => f.debug_struct("UInt32").finish(),
            BlockStorage::RoaringBitmap(_) => f.debug_struct("RoaringBitmap").finish(),
            BlockStorage::DataRecord(_) => f.debug_struct("DataRecord").finish(),
        }
    }
}

pub enum BlockKeyArrowBuilder {
    Boolean((StringBuilder, BooleanBuilder)),
    String((StringBuilder, StringBuilder)),
    Float32((StringBuilder, Float32Builder)),
    UInt32((StringBuilder, UInt32Builder)),
}

impl BlockKeyArrowBuilder {
    pub(crate) fn add_key(&mut self, key: CompositeKey) {
        match key.key {
            KeyWrapper::String(value) => {
                let builder = match self {
                    BlockKeyArrowBuilder::String(builder) => builder,
                    _ => {
                        unreachable!("Invariant violation. BlockKeyArrowBuilder should be String.")
                    }
                };
                builder.0.append_value(key.prefix);
                builder.1.append_value(value);
            }
            KeyWrapper::Float32(value) => {
                let builder = match self {
                    BlockKeyArrowBuilder::Float32(builder) => builder,
                    _ => {
                        unreachable!("Invariant violation. BlockKeyArrowBuilder should be Float32.")
                    }
                };
                builder.0.append_value(key.prefix);
                builder.1.append_value(value);
            }
            KeyWrapper::Bool(value) => {
                let builder = match self {
                    BlockKeyArrowBuilder::Boolean(builder) => builder,
                    _ => {
                        unreachable!("Invariant violation. BlockKeyArrowBuilder should be Boolean.")
                    }
                };
                builder.0.append_value(key.prefix);
                builder.1.append_value(value);
            }
            KeyWrapper::Uint32(value) => {
                let builder = match self {
                    BlockKeyArrowBuilder::UInt32(builder) => builder,
                    _ => {
                        unreachable!("Invariant violation. BlockKeyArrowBuilder should be UInt32.")
                    }
                };
                builder.0.append_value(key.prefix);
                builder.1.append_value(value);
            }
        }
    }

    pub fn as_arrow(&mut self) -> (Field, ArrayRef, Field, ArrayRef) {
        match self {
            BlockKeyArrowBuilder::String((ref mut prefix_builder, ref mut key_builder)) => {
                let prefix_field = Field::new("prefix", arrow::datatypes::DataType::Utf8, false);
                let key_field = Field::new("key", arrow::datatypes::DataType::Utf8, false);
                let prefix_arr = prefix_builder.finish();
                let key_arr = key_builder.finish();
                (
                    prefix_field,
                    (&prefix_arr as &dyn Array).slice(0, prefix_arr.len()),
                    key_field,
                    (&key_arr as &dyn Array).slice(0, key_arr.len()),
                )
            }
            BlockKeyArrowBuilder::Float32((ref mut prefix_builder, ref mut key_builder)) => {
                let prefix_field = Field::new("prefix", arrow::datatypes::DataType::Utf8, false);
                let key_field = Field::new("key", arrow::datatypes::DataType::Float32, false);
                let prefix_arr = prefix_builder.finish();
                let key_arr = key_builder.finish();
                (
                    prefix_field,
                    (&prefix_arr as &dyn Array).slice(0, prefix_arr.len()),
                    key_field,
                    (&key_arr as &dyn Array).slice(0, key_arr.len()),
                )
            }
            BlockKeyArrowBuilder::Boolean((ref mut prefix_builder, ref mut key_builder)) => {
                let prefix_field = Field::new("prefix", arrow::datatypes::DataType::Utf8, false);
                let key_field = Field::new("key", arrow::datatypes::DataType::Boolean, false);
                let prefix_arr = prefix_builder.finish();
                let key_arr = key_builder.finish();
                (
                    prefix_field,
                    (&prefix_arr as &dyn Array).slice(0, prefix_arr.len()),
                    key_field,
                    (&key_arr as &dyn Array).slice(0, key_arr.len()),
                )
            }
            BlockKeyArrowBuilder::UInt32((ref mut prefix_builder, ref mut key_builder)) => {
                let prefix_field = Field::new("prefix", arrow::datatypes::DataType::Utf8, false);
                let key_field = Field::new("key", arrow::datatypes::DataType::UInt32, false);
                let prefix_arr = prefix_builder.finish();
                let key_arr = key_builder.finish();
                (
                    prefix_field,
                    (&prefix_arr as &dyn Array).slice(0, prefix_arr.len()),
                    key_field,
                    (&key_arr as &dyn Array).slice(0, key_arr.len()),
                )
            }
        }
    }
}

impl BlockStorage {
    pub fn get_prefix_size(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_prefix_size(),
            BlockStorage::UInt32(builder) => builder.get_prefix_size(),
            BlockStorage::DataRecord(builder) => builder.get_prefix_size(),
            BlockStorage::VecUInt32(builder) => builder.get_prefix_size(),
            BlockStorage::RoaringBitmap(builder) => builder.get_prefix_size(),
        }
    }

    pub fn get_key_size(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_key_size(),
            BlockStorage::UInt32(builder) => builder.get_key_size(),
            BlockStorage::DataRecord(builder) => builder.get_key_size(),
            BlockStorage::VecUInt32(builder) => builder.get_key_size(),
            BlockStorage::RoaringBitmap(builder) => builder.get_key_size(),
        }
    }

    pub fn get_min_key(&self) -> Option<CompositeKey> {
        match self {
            BlockStorage::String(builder) => builder.get_min_key(),
            BlockStorage::UInt32(builder) => builder.get_min_key(),
            BlockStorage::DataRecord(builder) => builder.get_min_key(),
            BlockStorage::VecUInt32(builder) => builder.get_min_key(),
            BlockStorage::RoaringBitmap(builder) => builder.get_min_key(),
        }
    }

    /// Returns the arrow-padded (rounded to 64 bytes) size for the delta.
    pub fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_size::<K>(),
            BlockStorage::UInt32(builder) => builder.get_size::<K>(),
            BlockStorage::DataRecord(builder) => builder.get_size::<K>(),
            BlockStorage::VecUInt32(builder) => builder.get_size::<K>(),
            BlockStorage::RoaringBitmap(builder) => builder.get_size::<K>(),
        }
    }

    pub fn split<K: ArrowWriteableKey>(&self, split_size: usize) -> (CompositeKey, BlockStorage) {
        match self {
            BlockStorage::String(builder) => {
                let (split_key, storage) = builder.split::<K>(split_size);
                (split_key, BlockStorage::String(storage))
            }
            BlockStorage::UInt32(builder) => {
                let (split_key, storage) = builder.split::<K>(split_size);
                (split_key, BlockStorage::UInt32(storage))
            }
            BlockStorage::DataRecord(builder) => {
                let (split_key, storage) = builder.split::<K>(split_size);
                (split_key, BlockStorage::DataRecord(storage))
            }
            BlockStorage::VecUInt32(builder) => {
                let (split_key, storage) = builder.split::<K>(split_size);
                (split_key, BlockStorage::VecUInt32(storage))
            }
            BlockStorage::RoaringBitmap(builder) => {
                let (split_key, storage) = builder.split::<K>(split_size);
                (split_key, BlockStorage::RoaringBitmap(storage))
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.len(),
            BlockStorage::UInt32(builder) => builder.len(),
            BlockStorage::DataRecord(builder) => builder.len(),
            BlockStorage::VecUInt32(builder) => builder.len(),
            BlockStorage::RoaringBitmap(builder) => builder.len(),
        }
    }

    pub fn into_record_batch<K: ArrowWriteableKey>(
        self,
        metadata: Option<HashMap<String, String>>,
    ) -> RecordBatch {
        let key_builder =
            K::get_arrow_builder(self.len(), self.get_prefix_size(), self.get_key_size());
        match self {
            BlockStorage::String(builder) => {
                // TODO: handle error
                let (schema, columns) = builder.into_arrow(key_builder, metadata);
                RecordBatch::try_new(schema, columns).unwrap()
            }
            BlockStorage::UInt32(builder) => {
                // TODO: handle error
                let (schema, columns) = builder.into_arrow(key_builder, metadata);
                RecordBatch::try_new(schema, columns).unwrap()
            }
            BlockStorage::DataRecord(builder) => {
                // TODO: handle error
                builder.into_arrow(key_builder).unwrap()
            }
            BlockStorage::VecUInt32(builder) => {
                // TODO: handle error
                let (schema, columns) = builder.into_arrow(key_builder, metadata);
                RecordBatch::try_new(schema, columns).unwrap()
            }
            BlockStorage::RoaringBitmap(builder) => {
                // TODO: handle error
                let (schema, columns) = builder.into_arrow(key_builder, metadata);
                RecordBatch::try_new(schema, columns).unwrap()
            }
        }
    }
}
