use crate::errors::ChromaError;
use arrow::array::Int32Array;
use core::panic;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub(crate) struct BlockfileKey<K: PartialEq + PartialOrd + Clone> {
    prefix: String,
    key: K,
}

impl<K: Hash + PartialOrd + Clone> Hash for BlockfileKey<K> {
    // Hash is only used for the HashMap implementation, which is a test/reference implementation
    // Therefore this hash implementation is not used in production and allowed to be
    // hacky
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prefix.hash(state);
    }
}

impl<K: PartialOrd + Clone> PartialEq for BlockfileKey<K> {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.key == other.key
    }
}

impl<K: PartialOrd + Clone> Eq for BlockfileKey<K> {}

pub(crate) trait BlockfileValue {}

// TODO: align with rust collection conventions for this trait
pub(crate) trait SplittableBlockFileValue<V: BlockfileValue + PartialOrd>:
    BlockfileValue
{
    fn get_at_index(&self, index: usize) -> Result<&V, Box<dyn ChromaError>>;
    fn len(&self) -> usize;
}

pub(crate) trait Blockfile<K: PartialEq + PartialOrd + Clone, V: BlockfileValue> {
    // TODO: check the into string pattern
    fn open(path: &str) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;
    fn get(&self, key: BlockfileKey<K>) -> Result<&V, Box<dyn ChromaError>>;
    fn get_by_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>>;
    fn set(&mut self, key: BlockfileKey<K>, value: V) -> Result<(), Box<dyn ChromaError>>;

    // TODO: the naming of these methods are off since they don't mention the prefix
    // THOUGHT: make prefix optional and if its included, then it will be used to filter the results
    // Get all values with a given prefix where the key is greater than the given key
    fn get_gt(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>>;

    // Get all values with a given prefix where the key is less than the given key
    fn get_lt(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>>;

    // Get all values with a given prefix where the key is greater than or equal to the given key
    fn get_gte(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>>;

    fn get_lte(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>>;
}

pub(crate) trait SplittableBlockFile<
    K: PartialEq + PartialOrd + Clone,
    VV: BlockfileValue + PartialOrd,
    V: SplittableBlockFileValue<VV>,
>: Blockfile<K, V>
{
    fn get_with_value_hint(
        &self,
        key: BlockfileKey<K>,
        value_hint: VV,
    ) -> Result<&V, Box<dyn ChromaError>>;
}

struct HashMapBlockfile<K: PartialEq + PartialOrd + Clone, V> {
    map: std::collections::HashMap<BlockfileKey<K>, V>,
}

impl<K: PartialEq + PartialOrd + Hash + Clone, V: BlockfileValue> Blockfile<K, V>
    for HashMapBlockfile<K, V>
{
    // TODO: change this to respect path instead of ignoring it and creating a new thing
    fn open(_path: &str) -> Result<Self, Box<dyn ChromaError>> {
        Ok(HashMapBlockfile {
            map: std::collections::HashMap::new(),
        })
    }
    fn get(&self, key: BlockfileKey<K>) -> Result<&V, Box<dyn ChromaError>> {
        match self.map.get(&key) {
            Some(value) => Ok(value),
            None => {
                // TOOD: make error
                panic!("Key not found");
            }
        }
    }

    fn get_by_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        let mut result = Vec::new();
        for (key, value) in self.map.iter() {
            if key.prefix == prefix {
                result.push((key, value));
            }
        }
        Ok(result)
    }

    fn set(&mut self, key: BlockfileKey<K>, value: V) -> Result<(), Box<dyn ChromaError>> {
        self.map.insert(key, value);
        Ok(())
    }

    fn get_gt(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        let mut result = Vec::new();
        for (k, v) in self.map.iter() {
            if k.prefix == prefix && k.key > key {
                result.push((k, v));
            }
        }
        Ok(result)
    }

    fn get_gte(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        let mut result = Vec::new();
        for (k, v) in self.map.iter() {
            if k.prefix == prefix && k.key >= key {
                result.push((k, v));
            }
        }
        Ok(result)
    }

    fn get_lt(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        let mut result = Vec::new();
        for (k, v) in self.map.iter() {
            if k.prefix == prefix && k.key < key {
                result.push((k, v));
            }
        }
        Ok(result)
    }

    fn get_lte(
        &self,
        prefix: String,
        key: K,
    ) -> Result<Vec<(&BlockfileKey<K>, &V)>, Box<dyn ChromaError>> {
        let mut result = Vec::new();
        for (k, v) in self.map.iter() {
            if k.prefix == prefix && k.key <= key {
                result.push((k, v));
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::*;

    impl BlockfileValue for String {}
    impl Debug for BlockfileKey<String> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "BlockfileKey(prefix: {}, key: {})",
                self.prefix, self.key
            )
        }
    }

    #[test]
    fn test_blockfile_set_get() {
        let mut blockfile = HashMapBlockfile::open("test").unwrap();
        let key = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: "key1".to_string(),
        };
        let _res = blockfile.set(key.clone(), "value1".to_string()).unwrap();
        let value = blockfile.get(key);
        // downcast to string
        assert_eq!(value.unwrap(), "value1");
    }

    #[test]
    fn test_blockfile_get_by_prefix() {
        let mut blockfile = HashMapBlockfile::open("test").unwrap();
        let key1 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: "key1".to_string(),
        };
        let key2 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: "key2".to_string(),
        };
        let _res = blockfile.set(key1.clone(), "value1".to_string()).unwrap();
        let _res = blockfile.set(key2.clone(), "value2".to_string()).unwrap();
        let values = blockfile.get_by_prefix("text_prefix".to_string()).unwrap();
        assert_eq!(values.len(), 2);
        // May return values in any order
        assert!(values.contains(&(&key1.clone(), &"value1".to_string())));
        assert!(values.contains(&(&key2.clone(), &"value2".to_string())));
    }

    impl BlockfileValue for Int32Array {}
    impl BlockfileValue for i32 {}

    #[test]
    fn test_storing_arrow_in_blockfile() {
        let mut blockfile = HashMapBlockfile::open("test").unwrap();
        let key = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: "key1".to_string(),
        };
        let array = Int32Array::from(vec![1, 2, 3]);
        let _res = blockfile.set(key.clone(), array).unwrap();
        let value = blockfile.get(key).unwrap();
        assert_eq!(value, &Int32Array::from(vec![1, 2, 3]));
    }

    #[test]
    fn test_blockfile_get_gt() {
        let mut blockfile = HashMapBlockfile::open("test").unwrap();
        let key1 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: "key1".to_string(),
        };
        let key2 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: "key2".to_string(),
        };
        let key3 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: "key3".to_string(),
        };
        let _res = blockfile.set(key1.clone(), 1).unwrap();
        let _res = blockfile.set(key2.clone(), 2).unwrap();
        let _res = blockfile.set(key3.clone(), 3).unwrap();
        let values = blockfile
            .get_gt("text_prefix".to_string(), "key1".to_string())
            .unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&(&key2.clone(), &2)));
        assert!(values.contains(&(&key3.clone(), &3)));
    }
}
