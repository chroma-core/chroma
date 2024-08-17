use super::{
    data_record::DataRecordStorage, int32::Int32ArrayStorage, roaring_bitmap::RoaringBitmapStorage,
    string::StringValueStorage, uint32::UInt32Storage,
};
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
use std::{
    fmt,
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub enum BlockStorage {
    String(StringValueStorage),
    Int32Array(Int32ArrayStorage),
    UInt32(UInt32Storage),
    RoaringBitmap(RoaringBitmapStorage),
    DataRecord(DataRecordStorage),
}

impl Debug for BlockStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BlockStorage::String(_) => write!(f, "String"),
            BlockStorage::Int32Array(_) => write!(f, "Int32Array"),
            BlockStorage::UInt32(_) => write!(f, "UInt32"),
            BlockStorage::RoaringBitmap(_) => write!(f, "RoaringBitmap"),
            BlockStorage::DataRecord(_) => write!(f, "DataRecord"),
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
    pub(super) fn add_key(&mut self, key: CompositeKey) {
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

    fn to_arrow(&mut self) -> (Field, ArrayRef, Field, ArrayRef) {
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
    pub fn get_prefix_size(&self, start: usize, end: usize) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_prefix_size(start, end),
            BlockStorage::UInt32(builder) => builder.get_prefix_size(start, end),
            BlockStorage::DataRecord(builder) => builder.get_prefix_size(start, end),
            BlockStorage::Int32Array(builder) => builder.get_prefix_size(start, end),
            BlockStorage::RoaringBitmap(builder) => builder.get_prefix_size(start, end),
        }
    }

    pub fn get_key_size(&self, start: usize, end: usize) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_key_size(start, end),
            BlockStorage::UInt32(builder) => builder.get_key_size(start, end),
            BlockStorage::DataRecord(builder) => builder.get_key_size(start, end),
            BlockStorage::Int32Array(builder) => builder.get_key_size(start, end),
            BlockStorage::RoaringBitmap(builder) => builder.get_key_size(start, end),
        }
    }

    /// Returns the arrow-padded (rounded to 64 bytes) size of the value data for the given range.
    pub fn get_value_size(&self, start: usize, end: usize) -> usize {
        match self {
            BlockStorage::String(builder) => builder.get_value_size(start, end),
            BlockStorage::UInt32(builder) => builder.get_value_size(start, end),
            BlockStorage::DataRecord(builder) => builder.get_value_size(start, end),
            BlockStorage::Int32Array(builder) => builder.get_value_size(start, end),
            BlockStorage::RoaringBitmap(builder) => builder.get_value_size(start, end),
        }
    }

    pub fn split(&self, prefix: &str, key: KeyWrapper) -> BlockStorage {
        match self {
            BlockStorage::String(builder) => BlockStorage::String(builder.split(prefix, key)),
            BlockStorage::UInt32(builder) => BlockStorage::UInt32(builder.split(prefix, key)),
            BlockStorage::DataRecord(builder) => {
                BlockStorage::DataRecord(builder.split(prefix, key))
            }
            BlockStorage::Int32Array(builder) => {
                BlockStorage::Int32Array(builder.split(prefix, key))
            }
            BlockStorage::RoaringBitmap(builder) => {
                BlockStorage::RoaringBitmap(builder.split(prefix, key))
            }
        }
    }

    pub fn get_key(&self, index: usize) -> CompositeKey {
        match self {
            BlockStorage::String(builder) => {
                let storage = builder.storage.read();
                match storage.as_ref() {
                    None => unreachable!(
                        "Invariant violation. A StringValueBuilder should have storage."
                    ),
                    Some(storage) => {
                        let (key, _) = storage.iter().nth(index).unwrap();
                        key.clone()
                    }
                }
            }
            BlockStorage::UInt32(builder) => builder.get_key(index),
            BlockStorage::DataRecord(builder) => builder.get_key(index),
            BlockStorage::Int32Array(builder) => builder.get_key(index),
            BlockStorage::RoaringBitmap(builder) => builder.get_key(index),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            BlockStorage::String(builder) => builder.len(),
            BlockStorage::UInt32(builder) => builder.len(),
            BlockStorage::DataRecord(builder) => builder.len(),
            BlockStorage::Int32Array(builder) => builder.len(),
            BlockStorage::RoaringBitmap(builder) => builder.len(),
        }
    }

    pub fn to_record_batch<K: ArrowWriteableKey>(&self) -> RecordBatch {
        let mut key_builder = K::get_arrow_builder(
            self.len(),
            self.get_prefix_size(0, self.len()),
            self.get_key_size(0, self.len()),
        );
        match self {
            BlockStorage::String(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
            BlockStorage::UInt32(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
            BlockStorage::DataRecord(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
            BlockStorage::Int32Array(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
            BlockStorage::RoaringBitmap(builder) => {
                key_builder = builder.build_keys(key_builder);
            }
        }

        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.to_arrow();
        let (value_field, value_arr) = match self {
            BlockStorage::String(builder) => builder.to_arrow(),
            BlockStorage::UInt32(builder) => builder.to_arrow(),
            BlockStorage::DataRecord(builder) => builder.to_arrow(),
            BlockStorage::Int32Array(builder) => builder.to_arrow(),
            BlockStorage::RoaringBitmap(builder) => builder.to_arrow(),
        };
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            value_field,
        ]));
        let record_batch = RecordBatch::try_new(schema, vec![prefix_arr, key_arr, value_arr]);
        // TODO: handle error
        record_batch.unwrap()
    }
}

pub(super) fn calculate_prefix_size<'a>(
    composite_key_iter: impl Iterator<Item = &'a CompositeKey>,
) -> usize {
    composite_key_iter.fold(0, |acc, key| acc + key.prefix.len())
}

pub(super) fn calculate_key_size<'a>(
    composite_key_iter: impl Iterator<Item = &'a CompositeKey>,
) -> usize {
    composite_key_iter.fold(0, |acc, key| acc + key.key.get_size())
}
