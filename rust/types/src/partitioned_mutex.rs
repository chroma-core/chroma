use std::{
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

#[derive(Clone)]
pub struct AysncPartitionedMutex<K, V = (), H = DefaultHasher>
where
    K: Hash + Eq,
    H: Hasher + Default,
    V: Clone,
{
    partitions: Arc<[tokio::sync::Mutex<V>]>,
    _hasher: std::marker::PhantomData<H>,
    _key: std::marker::PhantomData<K>,
}

// TODO: A sensible value for this.
const DEFAULT_NUM_PARTITIONS: usize = 16 * 16;

impl<K, V, H> AysncPartitionedMutex<K, V, H>
where
    K: Hash + Eq,
    H: Hasher + Default,
    V: Clone,
{
    pub fn new(default_value: V) -> Self {
        let partitions = (0..DEFAULT_NUM_PARTITIONS)
            .map(|_| tokio::sync::Mutex::new(default_value.clone()))
            .collect::<Vec<_>>();
        Self {
            partitions: partitions.into(),
            _hasher: PhantomData,
            _key: PhantomData,
        }
    }

    fn with_partitions(num_partitions: usize, default_value: V) -> Self {
        let partitions = (0..num_partitions)
            .map(|_| tokio::sync::Mutex::new(default_value.clone()))
            .collect::<Vec<_>>();
        Self {
            partitions: partitions.into(),
            _hasher: PhantomData,
            _key: PhantomData,
        }
    }

    pub fn with_workers(num_workers: usize, default_value: V) -> Self {
        Self::with_partitions(num_workers * num_workers, default_value)
    }

    pub async fn lock(&self, key: &K) -> tokio::sync::MutexGuard<'_, V> {
        let mut hasher = H::default();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        self.partitions[hash % self.partitions.len()].lock().await
    }
}
