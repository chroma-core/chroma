use super::{
    builder_storage::{BuilderStorage, DeferredSortStorage, VecBuilderStorage},
    single_column_size_tracker::SingleColumnSizeTracker,
    BlockKeyArrowBuilder,
};
use crate::{
    arrow::types::{ArrowWriteableKey, ArrowWriteableValue},
    key::{CompositeKey, KeyWrapper},
    BlockfileWriterMutationOrdering,
};
use arrow::util::bit_util;
use arrow::{array::Array, datatypes::Schema};
use parking_lot::RwLock;
use std::sync::Arc;
use std::{collections::HashMap, vec};

#[derive(Clone)]
pub struct SingleColumnStorage<T: ArrowWriteableValue> {
    inner: Arc<RwLock<Inner<T>>>,
}

struct Inner<V: ArrowWriteableValue> {
    storage: BuilderStorage<V>,
    size_tracker: SingleColumnSizeTracker,
}

impl<V: ArrowWriteableValue> std::fmt::Debug for Inner<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("size_tracker", &self.size_tracker)
            .finish()
    }
}

impl<V: ArrowWriteableValue<SizeTracker = SingleColumnSizeTracker>> SingleColumnStorage<V> {
    pub(in crate::arrow) fn new(mutation_ordering_hint: BlockfileWriterMutationOrdering) -> Self {
        let storage = match mutation_ordering_hint {
            BlockfileWriterMutationOrdering::Unordered => {
                BuilderStorage::DeferredSortStorage(DeferredSortStorage::default())
            }
            BlockfileWriterMutationOrdering::Ordered => {
                BuilderStorage::VecBuilderStorage(VecBuilderStorage::default())
            }
        };

        Self {
            inner: Arc::new(RwLock::new(Inner {
                storage,
                size_tracker: SingleColumnSizeTracker::new(),
            })),
        }
    }

    pub(super) fn get_prefix_size(&self) -> usize {
        let inner = self.inner.read();
        inner.size_tracker.get_prefix_size()
    }

    pub(super) fn get_key_size(&self) -> usize {
        let inner = self.inner.read();
        inner.size_tracker.get_key_size()
    }

    pub(super) fn len(&self) -> usize {
        let inner = self.inner.read();
        inner.storage.len()
    }

    pub fn get_min_key(&self) -> Option<CompositeKey> {
        let mut inner = self.inner.write();
        inner.storage.min_key().cloned()
    }

    pub(super) fn get_size<K: ArrowWriteableKey>(&self) -> usize {
        let inner = self.inner.read();

        let prefix_size = inner.size_tracker.get_arrow_padded_prefix_size();
        let key_size = inner.size_tracker.get_arrow_padded_key_size();
        let value_size = inner.size_tracker.get_arrow_padded_value_size();

        // offset sizing
        // https://docs.rs/arrow-buffer/52.2.0/arrow_buffer/buffer/struct.OffsetBuffer.html
        // 4 bytes per offset entry, n+1 entries
        let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((self.len() + 1) * 4);
        let key_offset_bytes: usize = K::offset_size(self.len());

        let value_offset_bytes = V::offset_size(self.len());
        let value_validity_bytes = V::validity_size(self.len());

        prefix_size
            + key_size
            + value_size
            + prefix_offset_bytes
            + key_offset_bytes
            + value_offset_bytes
            + value_validity_bytes
    }

    /// Add a key-value pair to the storage.
    /// If the key already exists, the old value is replaced.
    #[inline]
    pub fn add(&self, prefix: &str, key: KeyWrapper, value: V) {
        let mut inner = self.inner.write();
        let key_len = key.get_size();
        let prefix_len = prefix.len();

        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };

        // Check if key exists and delete old value
        if let Some(old_value) = inner.storage.delete(&composite_key) {
            let old_value_size = old_value.get_size();
            inner.size_tracker.subtract_value_size(old_value_size);
            inner.size_tracker.subtract_key_size(key_len);
            inner.size_tracker.subtract_prefix_size(prefix_len);
            inner.size_tracker.decrement_item_count();
        }

        let value_size = value.get_size();

        inner.storage.add(composite_key, value);
        inner.size_tracker.add_prefix_size(prefix_len);
        inner.size_tracker.add_key_size(key_len);
        inner.size_tracker.add_value_size(value_size);
        inner.size_tracker.increment_item_count();
    }

    /// Add multiple key-value pairs in a single batch operation.
    /// This is more efficient than calling `add` multiple times because it
    /// acquires the write lock only once and amortizes the lock overhead.
    pub fn batch_add<I>(&self, items: I)
    where
        I: IntoIterator<Item = (String, KeyWrapper, V)>,
    {
        let mut inner = self.inner.write();
        for (prefix, key, value) in items {
            let key_len = key.get_size();
            let prefix_len = prefix.len();

            let composite_key = CompositeKey { prefix, key };

            if let Some(old_value) = inner.storage.delete(&composite_key) {
                let old_value_size = old_value.get_size();
                inner.size_tracker.subtract_value_size(old_value_size);
                inner.size_tracker.subtract_key_size(key_len);
                inner.size_tracker.subtract_prefix_size(prefix_len);
                inner.size_tracker.decrement_item_count();
            }

            let value_size = value.get_size();

            inner.storage.add(composite_key, value);
            inner.size_tracker.add_prefix_size(prefix_len);
            inner.size_tracker.add_key_size(key_len);
            inner.size_tracker.add_value_size(value_size);
            inner.size_tracker.increment_item_count();
        }
    }

    pub fn delete(&self, prefix: &str, key: KeyWrapper) {
        let mut inner = self.inner.write();
        let maybe_removed_prefix_len = prefix.len();
        let maybe_removed_key_len = key.get_size();
        let maybe_removed_value = inner.storage.delete(&CompositeKey {
            prefix: prefix.to_string(),
            key,
        });

        if let Some(value) = maybe_removed_value {
            inner
                .size_tracker
                .subtract_prefix_size(maybe_removed_prefix_len);
            inner.size_tracker.subtract_key_size(maybe_removed_key_len);
            inner.size_tracker.subtract_value_size(value.get_size());
            inner.size_tracker.decrement_item_count();
        }
    }

    pub(super) fn split<K: ArrowWriteableKey>(
        &self,
        split_size: usize,
    ) -> (CompositeKey, SingleColumnStorage<V>) {
        let mut inner = self.inner.write();

        let mut num_items = 0;
        let mut prefix_size = 0;
        let mut key_size = 0;
        let mut value_size = 0;
        let mut split_key = None;

        {
            let storage = &mut inner.storage;

            let mut item_count = 0;
            let mut iter = storage.iter();
            let mut last_key: Option<&CompositeKey> = None;
            while let Some((key, value)) = iter.next() {
                // TODO: we seem to have a concurrency bug somewhere that means inner.storage may not always be ordered (when it's backed by a Vec). This is a temporary check that provides additional debugging information if the bug happens again. This should be removed once we fix the underlying bug.
                if let Some(last_key) = &last_key {
                    if key < last_key {
                        panic!("Keys are not in order. Scanned up to {num_items}. Found {key:?} after {last_key:?}.");
                    }
                } else {
                    last_key = Some(key)
                }

                num_items += 1;
                prefix_size += key.prefix.len();
                key_size += key.key.get_size();
                value_size += value.get_size();
                item_count += 1;

                // offset sizing
                let prefix_offset_bytes = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);
                let key_offset_bytes = K::offset_size(item_count);
                let value_offset_bytes = bit_util::round_upto_multiple_of_64((item_count + 1) * 4);

                // validitiy sizing
                let value_validity_bytes = V::validity_size(item_count);

                let total_size = bit_util::round_upto_multiple_of_64(prefix_size)
                    + bit_util::round_upto_multiple_of_64(key_size)
                    + bit_util::round_upto_multiple_of_64(value_size)
                    + prefix_offset_bytes
                    + key_offset_bytes
                    + value_offset_bytes
                    + value_validity_bytes;

                if total_size > split_size {
                    split_key = match iter.next() {
                        None => {
                            // Remove the last item since we are splitting at the end
                            prefix_size -= key.prefix.len();
                            key_size -= key.key.get_size();
                            value_size -= value.get_size();
                            Some(key.clone())
                        }
                        Some((next_key, _)) => Some(next_key.clone()),
                    };
                    break;
                }
            }
        }

        let total_num_items = inner.size_tracker.get_num_items();
        let total_prefix_size = inner.size_tracker.get_prefix_size();
        let total_key_size = inner.size_tracker.get_key_size();
        let total_value_size = inner.size_tracker.get_value_size();
        inner
            .size_tracker
            .subtract_prefix_size(total_prefix_size - prefix_size);
        inner
            .size_tracker
            .subtract_key_size(total_key_size - key_size);
        inner
            .size_tracker
            .subtract_value_size(total_value_size - value_size);
        inner
            .size_tracker
            .subtract_item_count(total_num_items - num_items);

        match split_key {
            None => panic!("A storage should have at least one element to be split."),
            Some(split_key) => {
                let new_delta = inner.storage.split_off(&split_key);
                (
                    split_key,
                    SingleColumnStorage {
                        inner: Arc::new(RwLock::new(Inner {
                            storage: new_delta,
                            size_tracker: SingleColumnSizeTracker::with_values(
                                total_num_items - num_items,
                                total_prefix_size - prefix_size,
                                total_key_size - key_size,
                                total_value_size - value_size,
                            ),
                        })),
                    },
                )
            }
        }
    }

    pub(super) fn into_arrow(
        self,
        mut key_builder: BlockKeyArrowBuilder,
        metadata: Option<HashMap<String, String>>,
    ) -> (Arc<Schema>, Vec<Arc<dyn Array>>) {
        let inner = Arc::try_unwrap(self.inner)
            .expect(
                "Invariant violation: SingleColumnStorage inner should have only one reference.",
            )
            .into_inner();

        let mut value_builder = V::get_arrow_builder(inner.size_tracker.clone());

        let storage = inner.storage;
        for (key, value) in storage.into_iter() {
            key_builder.add_key(key);
            V::append(V::prepare(value), &mut value_builder);
        }

        let (prefix_field, prefix_arr, key_field, key_arr) = key_builder.as_arrow();
        let (value_field, value_arr) = V::finish(value_builder, &inner.size_tracker);
        let schema = arrow::datatypes::Schema::new(vec![prefix_field, key_field, value_field]);

        if let Some(metadata) = metadata {
            let schema = schema.with_metadata(metadata);
            return (schema.into(), vec![prefix_arr, key_arr, value_arr]);
        }

        (schema.into(), vec![prefix_arr, key_arr, value_arr])
    }

    pub fn get_owned_value(&self, prefix: &str, key: KeyWrapper) -> Option<V::PreparedValue> {
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };
        self.inner.read().storage.get(&composite_key)
    }

    /// Returns a reference to the value without copying it.
    /// The closure `f` is called with a reference to the value if it exists.
    /// This allows zero-copy access to the stored data.
    pub fn with_value<R>(&self, prefix: &str, key: KeyWrapper, f: impl FnOnce(Option<&V>) -> R) -> R {
        let composite_key = CompositeKey {
            prefix: prefix.to_string(),
            key,
        };
        let inner = self.inner.read();
        f(inner.storage.get_ref(&composite_key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Instant;

    #[test]
    fn benchmark_deferred_sort_vs_btreemap() {
        // This test compares DeferredSortStorage (our new impl) vs BTreeMap (old impl)
        let iterations = 100_000;

        // Pre-create data to avoid measuring allocation time
        let data: Vec<_> = (0..iterations)
            .map(|i| {
                (
                    CompositeKey {
                        prefix: "prefix".to_string(),
                        key: KeyWrapper::String(format!("key{}", i)),
                    },
                    format!("value{}", i),
                )
            })
            .collect();

        // Test DeferredSortStorage with dedup checking
        let mut deferred_storage: super::super::builder_storage::DeferredSortStorage<String> =
            Default::default();
        let data_clone1: Vec<_> = data.clone();
        let start = Instant::now();
        for (key, value) in data_clone1 {
            deferred_storage.add(key, value);
        }
        let deferred_insert_time = start.elapsed();

        // Measure time to iterate (which triggers sorting)
        let start = Instant::now();
        let _count: usize = deferred_storage.iter().count();
        let deferred_sort_time = start.elapsed();

        // Test DeferredSortStorage with add_unchecked (no dedup - fastest path)
        let mut deferred_unchecked: super::super::builder_storage::DeferredSortStorage<String> =
            Default::default();
        let data_clone_unchecked: Vec<_> = data.clone();
        let start = Instant::now();
        for (key, value) in data_clone_unchecked {
            deferred_unchecked.add_unchecked(key, value);
        }
        let unchecked_insert_time = start.elapsed();

        // Measure sort time for unchecked
        let start = Instant::now();
        let _count: usize = deferred_unchecked.iter().count();
        let unchecked_sort_time = start.elapsed();

        // Test BTreeMap (O(log n) per insert)
        let mut btree_storage: BTreeMap<CompositeKey, String> = BTreeMap::new();
        let data_clone2: Vec<_> = data.clone();
        let start = Instant::now();
        for (key, value) in data_clone2 {
            btree_storage.insert(key, value);
        }
        let btree_time = start.elapsed();

        // Calculate speedups
        let total_deferred = deferred_insert_time + deferred_sort_time;
        let total_unchecked = unchecked_insert_time + unchecked_sort_time;
        let insert_speedup = btree_time.as_nanos() as f64 / deferred_insert_time.as_nanos() as f64;
        let unchecked_insert_speedup = btree_time.as_nanos() as f64 / unchecked_insert_time.as_nanos() as f64;
        let total_speedup = btree_time.as_nanos() as f64 / total_deferred.as_nanos() as f64;
        let unchecked_total_speedup = btree_time.as_nanos() as f64 / total_unchecked.as_nanos() as f64;

        println!("\n=== DeferredSortStorage vs BTreeMap ({} items) ===", iterations);
        println!("BTreeMap inserts: {:?}", btree_time);
        println!();
        println!("DeferredSort (with dedup check):");
        println!("  Inserts: {:?}", deferred_insert_time);
        println!("  Sort: {:?}", deferred_sort_time);
        println!("  Total: {:?}", total_deferred);
        println!("  Insert speedup: {:.2}x", insert_speedup);
        println!("  Total speedup: {:.2}x", total_speedup);
        println!();
        println!("DeferredSort (unchecked - no dedup):");
        println!("  Inserts: {:?}", unchecked_insert_time);
        println!("  Sort: {:?}", unchecked_sort_time);
        println!("  Total: {:?}", total_unchecked);
        println!("  Insert speedup: {:.2}x", unchecked_insert_speedup);
        println!("  Total speedup: {:.2}x", unchecked_total_speedup);
        println!("=================================================\n");

        // DeferredSort inserts should be faster than BTreeMap inserts
        assert!(
            deferred_insert_time < btree_time,
            "DeferredSort inserts should be faster than BTreeMap inserts"
        );
    }

    #[test]
    fn benchmark_individual_vs_batch_add() {
        // This test compares individual add() calls vs batch_add()
        let iterations = 10_000;

        // Test individual adds
        let storage: SingleColumnStorage<String> =
            SingleColumnStorage::new(BlockfileWriterMutationOrdering::Unordered);
        let start = Instant::now();
        for i in 0..iterations {
            storage.add(
                "prefix",
                KeyWrapper::String(format!("key{}", i)),
                format!("value{}", i),
            );
        }
        let individual_time = start.elapsed();

        // Test batch adds
        let storage2: SingleColumnStorage<String> =
            SingleColumnStorage::new(BlockfileWriterMutationOrdering::Unordered);
        let items: Vec<_> = (0..iterations)
            .map(|i| {
                (
                    "prefix".to_string(),
                    KeyWrapper::String(format!("key{}", i)),
                    format!("value{}", i),
                )
            })
            .collect();
        let start = Instant::now();
        storage2.batch_add(items);
        let batch_time = start.elapsed();

        let speedup = individual_time.as_nanos() as f64 / batch_time.as_nanos() as f64;

        println!(
            "\n=== Performance Comparison ({} items) ===",
            iterations
        );
        println!("Individual adds: {:?}", individual_time);
        println!("Batch add: {:?}", batch_time);
        println!("Speedup: {:.2}x", speedup);
        println!("==========================================\n");

        // The batch should be faster (at least some improvement)
        // The actual speedup depends on the workload characteristics
        assert!(
            batch_time <= individual_time,
            "Batch add should not be slower than individual adds"
        );
    }
}
