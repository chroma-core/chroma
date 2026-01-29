use std::{collections::BTreeMap, mem::size_of, sync::Arc};

use arrow::{array::RecordBatch, util::bit_util};
use chroma_types::{QuantizedCluster, QuantizedClusterOwned};
use parking_lot::RwLock;

use crate::{
    arrow::{
        block::value::quantized_cluster_value::QuantizedClusterSizeTracker,
        types::{ArrowWriteableKey, ArrowWriteableValue},
    },
    key::{CompositeKey, KeyWrapper},
};

use super::BlockKeyArrowBuilder;

#[derive(Debug)]
struct Inner {
    storage: BTreeMap<CompositeKey, QuantizedClusterOwned>,
    key_size: usize,
    prefix_size: usize,
    vector_count: usize,
}

#[derive(Debug, Clone)]
pub struct QuantizedClusterDelta {
    inner: Arc<RwLock<Inner>>,
}

impl QuantizedClusterDelta {
    pub(in crate::arrow) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                storage: BTreeMap::new(),
                key_size: 0,
                prefix_size: 0,
                vector_count: 0,
            })),
        }
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        self.inner.read().prefix_size
    }

    pub(super) fn get_key_size(&self) -> usize {
        self.inner.read().key_size
    }

    pub fn get_owned_value(&self, prefix: &str, key: KeyWrapper) -> Option<QuantizedClusterOwned> {
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };
        self.inner.read().storage.get(&composite_key).cloned()
    }

    pub fn add(&self, prefix: &str, key: KeyWrapper, value: QuantizedCluster<'_>) {
        let mut inner = self.inner.write();
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        if let Some(old) = inner.storage.remove(&composite_key) {
            inner.prefix_size -= composite_key.prefix.len();
            inner.key_size -= composite_key.key.get_size();
            inner.vector_count -= old.ids.len();
        }

        inner.prefix_size += composite_key.prefix.len();
        inner.key_size += composite_key.key.get_size();
        inner.vector_count += value.ids.len();
        inner
            .storage
            .insert(composite_key, QuantizedClusterOwned::from(value));
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
            inner.vector_count -= old.ids.len();
        }
    }

    pub(super) fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        let inner = self.inner.read();
        let cluster_count = inner.storage.len();

        let (dimension, code_length) = inner
            .storage
            .values()
            .next()
            .map(|v| (v.center.len(), v.codes.len() / v.ids.len().max(1)))
            .unwrap_or((0, 0));

        // Size breakdown:
        // - center: FixedSizeList has no offsets, just data
        // - codes: outer List has offsets, inner FixedSizeList has no offsets
        // - ids/versions: List has offsets
        // Total offset arrays: 3 (codes outer, ids, versions)
        bit_util::round_upto_multiple_of_64(inner.prefix_size)
            + bit_util::round_upto_multiple_of_64(inner.key_size)
            + bit_util::round_upto_multiple_of_64(cluster_count * dimension * size_of::<f32>())
            + bit_util::round_upto_multiple_of_64(inner.vector_count * code_length)
            + bit_util::round_upto_multiple_of_64(inner.vector_count * size_of::<u64>()) * 2
            + bit_util::round_upto_multiple_of_64((cluster_count + 1) * 4) * 3
            + K::offset_size(cluster_count)
    }

    pub(super) fn split<K: ArrowWriteableKey>(
        &self,
        split_size: usize,
    ) -> (CompositeKey, QuantizedClusterDelta) {
        let split_key = self.find_split_key::<K>(split_size);
        let mut inner = self.inner.write();

        let new_storage = inner.storage.split_off(&split_key);

        let mut new_prefix_size = 0;
        let mut new_key_size = 0;
        let mut new_vector_count = 0;
        for (k, v) in &new_storage {
            new_prefix_size += k.prefix.len();
            new_key_size += k.key.get_size();
            new_vector_count += v.ids.len();
        }

        inner.prefix_size -= new_prefix_size;
        inner.key_size -= new_key_size;
        inner.vector_count -= new_vector_count;

        (
            split_key,
            QuantizedClusterDelta {
                inner: Arc::new(RwLock::new(Inner {
                    storage: new_storage,
                    key_size: new_key_size,
                    prefix_size: new_prefix_size,
                    vector_count: new_vector_count,
                })),
            },
        )
    }

    fn find_split_key<K: ArrowWriteableKey>(&self, split_size: usize) -> CompositeKey {
        let inner = self.inner.read();

        let (dimension, code_length) = inner
            .storage
            .values()
            .next()
            .map(|v| (v.center.len(), v.codes.len() / v.ids.len().max(1)))
            .unwrap_or((0, 0));

        let mut prefix_size = 0;
        let mut key_size = 0;
        let mut vector_count = 0;
        let mut cluster_count = 0;

        for (k, v) in &inner.storage {
            prefix_size += k.prefix.len();
            key_size += k.key.get_size();
            vector_count += v.ids.len();
            cluster_count += 1;

            // Same size calculation as get_size()
            let total_size = bit_util::round_upto_multiple_of_64(prefix_size)
                + bit_util::round_upto_multiple_of_64(key_size)
                + bit_util::round_upto_multiple_of_64(cluster_count * dimension * size_of::<f32>())
                + bit_util::round_upto_multiple_of_64(vector_count * code_length)
                + bit_util::round_upto_multiple_of_64(vector_count * size_of::<u64>()) * 2
                + bit_util::round_upto_multiple_of_64((cluster_count + 1) * 4) * 3
                + K::offset_size(cluster_count);

            if total_size > split_size {
                return k.clone();
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
            .expect("QuantizedClusterDelta inner should have only one reference")
            .into_inner();

        let (dimension, code_length) = inner
            .storage
            .values()
            .next()
            .map(|v| (v.center.len(), v.codes.len() / v.ids.len().max(1)))
            .unwrap_or((0, 0));

        let size_tracker = QuantizedClusterSizeTracker {
            cluster_count: inner.storage.len(),
            code_length,
            dimension,
            vector_count: inner.vector_count,
        };

        let mut value_builder =
            <QuantizedCluster as ArrowWriteableValue>::get_arrow_builder(size_tracker.clone());

        for (key, value) in inner.storage.into_iter() {
            key_builder.add_key(key);
            <QuantizedCluster as ArrowWriteableValue>::append(value, &mut value_builder);
        }

        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        let (struct_field, value_arr) =
            <QuantizedCluster as ArrowWriteableValue>::finish(value_builder, &size_tracker);

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            struct_field,
        ]));
        RecordBatch::try_new(schema, vec![prefix_arr, key_arr, value_arr])
    }
}
