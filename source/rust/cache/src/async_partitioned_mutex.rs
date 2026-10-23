/// This is a basic partitioned mutex that is not persistent.
/// The mutex is designed to be holdable across await points (hence async).
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

#[derive(Clone, Debug)]
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
const DEFAULT_NUM_PARTITIONS: usize = 32768;

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

    // Internally the number of partitions of the partitioned mutex
    // that is used to synchronize concurrent loads is set to
    // permitted_parallelism * permitted_parallelism. This is
    // inspired by the birthday paradox.
    pub fn with_parallelism(parallelism: usize, default_value: V) -> Self {
        Self::with_partitions(parallelism * parallelism, default_value)
    }

    pub async fn lock(&self, key: &K) -> tokio::sync::MutexGuard<'_, V> {
        let mut hasher = H::default();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        self.partitions[hash % self.partitions.len()].lock().await
    }
}
