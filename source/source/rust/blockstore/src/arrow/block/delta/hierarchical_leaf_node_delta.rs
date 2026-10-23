use std::{collections::BTreeMap, mem::size_of, sync::Arc};

use arrow::{array::RecordBatch, util::bit_util};
use chroma_types::hierarchical_spann::{HierarchicalLeafNode, HierarchicalLeafNodeOwned};
use parking_lot::RwLock;

use crate::{
    arrow::{
        block::value::hierarchical_leaf_node_value::HierarchicalLeafNodeSizeTracker,
        types::{ArrowWriteableKey, ArrowWriteableValue},
    },
    key::{CompositeKey, KeyWrapper},
};

use super::BlockKeyArrowBuilder;

#[derive(Debug)]
struct Inner {
    storage: BTreeMap<CompositeKey, HierarchicalLeafNodeOwned>,
    key_size: usize,
    prefix_size: usize,
    node_count: usize,
    total_code_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct HierarchicalLeafNodeDelta {
    inner: Arc<RwLock<Inner>>,
}

impl HierarchicalLeafNodeDelta {
    pub(in crate::arrow) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                storage: BTreeMap::new(),
                key_size: 0,
                prefix_size: 0,
                node_count: 0,
                total_code_bytes: 0,
            })),
        }
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        self.inner.read().prefix_size
    }

    pub(super) fn get_key_size(&self) -> usize {
        self.inner.read().key_size
    }

    pub fn get_owned_value(
        &self,
        prefix: &str,
        key: KeyWrapper,
    ) -> Option<HierarchicalLeafNodeOwned> {
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };
        self.inner.read().storage.get(&composite_key).cloned()
    }

    pub fn add(&self, prefix: &str, key: KeyWrapper, value: HierarchicalLeafNode<'_>) {
        let mut inner = self.inner.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        if let Some(old) = inner.storage.remove(&composite_key) {
            inner.prefix_size -= composite_key.prefix.len();
            inner.key_size -= composite_key.key.get_size();
            inner.node_count -= 1;
            inner.total_code_bytes -= old.centroid_code.len();
        }

        inner.prefix_size += composite_key.prefix.len();
        inner.key_size += composite_key.key.get_size();
        inner.node_count += 1;
        inner.total_code_bytes += value.centroid_code.len();
        inner
            .storage
            .insert(composite_key, HierarchicalLeafNodeOwned::from(value));
    }

    pub fn delete(&self, prefix: &str, key: KeyWrapper) {
        let mut inner = self.inner.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        if let Some(old) = inner.storage.remove(&composite_key) {
            inner.prefix_size -= composite_key.prefix.len();
            inner.key_size -= composite_key.key.get_size();
            inner.node_count -= 1;
            inner.total_code_bytes -= old.centroid_code.len();
        }
    }

    pub(super) fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        let inner = self.inner.read();
        let node_count = inner.storage.len();

        // Arrow size breakdown (each component 64-byte aligned):
        // - prefix_size / key_size: composite key strings and data
        // - parent:        node_count * 4 (UInt32Array, no offsets)
        // - length:        node_count * 4 (UInt32Array, no offsets)
        // - centroid_code: BinaryArray — byte data + (node_count+1) i32 offsets
        // - key offsets:   key-type-specific overhead
        bit_util::round_upto_multiple_of_64(inner.prefix_size)
            + bit_util::round_upto_multiple_of_64(inner.key_size)
            + bit_util::round_upto_multiple_of_64(node_count * size_of::<u32>()) // parent
            + bit_util::round_upto_multiple_of_64(node_count * size_of::<u32>()) // length
            + bit_util::round_upto_multiple_of_64(inner.total_code_bytes) // centroid_code bytes
            + bit_util::round_upto_multiple_of_64((node_count + 1) * size_of::<i32>()) // Binary offsets
            + K::offset_size(node_count)
    }

    pub(super) fn split<K: ArrowWriteableKey>(
        &self,
        split_size: usize,
    ) -> (CompositeKey, HierarchicalLeafNodeDelta) {
        let split_key = self.find_split_key::<K>(split_size);
        let mut inner = self.inner.write();

        let new_storage = inner.storage.split_off(&split_key);

        let mut new_prefix_size = 0;
        let mut new_key_size = 0;
        let mut new_node_count = 0;
        let mut new_total_code_bytes = 0;
        for (k, v) in &new_storage {
            new_prefix_size += k.prefix.len();
            new_key_size += k.key.get_size();
            new_node_count += 1;
            new_total_code_bytes += v.centroid_code.len();
        }

        inner.prefix_size -= new_prefix_size;
        inner.key_size -= new_key_size;
        inner.node_count -= new_node_count;
        inner.total_code_bytes -= new_total_code_bytes;

        (
            split_key,
            HierarchicalLeafNodeDelta {
                inner: Arc::new(RwLock::new(Inner {
                    storage: new_storage,
                    key_size: new_key_size,
                    prefix_size: new_prefix_size,
                    node_count: new_node_count,
                    total_code_bytes: new_total_code_bytes,
                })),
            },
        )
    }

    fn find_split_key<K: ArrowWriteableKey>(&self, split_size: usize) -> CompositeKey {
        let inner = self.inner.read();

        let mut prefix_size = 0;
        let mut key_size = 0;
        let mut node_count = 0;
        let mut total_code_bytes = 0;

        let mut iter = inner.storage.iter().peekable();
        while let Some((k, v)) = iter.next() {
            prefix_size += k.prefix.len();
            key_size += k.key.get_size();
            node_count += 1;
            total_code_bytes += v.centroid_code.len();

            let total_size = bit_util::round_upto_multiple_of_64(prefix_size)
                + bit_util::round_upto_multiple_of_64(key_size)
                + bit_util::round_upto_multiple_of_64(node_count * size_of::<u32>())
                + bit_util::round_upto_multiple_of_64(node_count * size_of::<u32>())
                + bit_util::round_upto_multiple_of_64(total_code_bytes)
                + bit_util::round_upto_multiple_of_64((node_count + 1) * size_of::<i32>())
                + K::offset_size(node_count);

            if total_size > split_size {
                return match iter.peek() {
                    Some((next_key, _)) => (*next_key).clone(),
                    None => k.clone(),
                };
            }
        }

        unreachable!("Split key not found")
    }

    pub fn get_min_key(&self) -> Option<CompositeKey> {
        self.inner.read().storage.keys().next().cloned()
    }

    pub(super) fn len(&self) -> usize {
        self.inner.read().storage.len()
    }

    pub(super) fn into_arrow(
        self,
        mut key_builder: BlockKeyArrowBuilder,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        let inner = Arc::try_unwrap(self.inner)
            .expect("HierarchicalLeafNodeDelta inner should have only one reference")
            .into_inner();

        let size_tracker = HierarchicalLeafNodeSizeTracker {
            node_count: inner.storage.len(),
            total_code_bytes: inner.total_code_bytes,
        };

        let mut value_builder =
            <HierarchicalLeafNode as ArrowWriteableValue>::get_arrow_builder(size_tracker.clone());

        for (key, value) in inner.storage.into_iter() {
            key_builder.add_key(key);
            <HierarchicalLeafNode as ArrowWriteableValue>::append(value, &mut value_builder);
        }

        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        let (struct_field, value_arr) =
            <HierarchicalLeafNode as ArrowWriteableValue>::finish(value_builder, &size_tracker);

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            struct_field,
        ]));
        RecordBatch::try_new(schema, vec![prefix_arr, key_arr, value_arr])
    }
}
