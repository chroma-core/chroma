use crate::errors::ChromaError;
use arrow::array::{
    Array, ArrayRef, Int32Array, Int32Builder, ListArray, ListBuilder, StructArray, StructBuilder,
};
use arrow::datatypes::{DataType, Field};
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

// struct SparseIndex<K: PartialEq + PartialOrd + Clone, V> {
//     boundaries: arrow::datatypes::DataType::Struct,
// }

// struct ParquetBlockfile<K: PartialEq + PartialOrd + Clone, V> {}

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, sync::Arc};

    use k8s_openapi::List;
    use prost_types::Struct;

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

    impl BlockfileValue for StructArray {}

    #[test]
    fn test_learning_arrow_struct() {
        // positional inverted index is term -> doc_ids -> positions
        // lets construct ["term1", "term2"] -> [[1, 2, 3], [4]] -> [[[0], [0, 1], [0, 1, 2]], [[10]]]
        // this is implemented as two KV
        // term1 -> Struct(ids: [1,2,3], pos: [[0], [0, 1], [0, 1, 2]])
        // term2 -> Struct(ids: [4], pos: [[10]])
        let mut id_list_builder = Int32Builder::new();
        id_list_builder.append_value(1);
        id_list_builder.append_value(2);
        id_list_builder.append_value(3);
        let id_list = id_list_builder.finish();

        let mut pos_list_builder = ListBuilder::new(Int32Builder::new());
        // Create the first list [[0], [0, 1], [0, 1, 2]]
        let term1 = pos_list_builder.values();
        term1.append_value(0);
        pos_list_builder.append(true);
        let term1 = pos_list_builder.values();
        term1.append_value(0);
        term1.append_value(1);
        pos_list_builder.append(true);
        let term1 = pos_list_builder.values();
        term1.append_value(0);
        term1.append_value(1);
        term1.append_value(2);
        pos_list_builder.append(true);

        // TODO: build the ids such that they don't have to be named "item" and be nullable
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("id_list", DataType::Int32, true)),
                Arc::new(id_list.clone()) as ArrayRef,
            ),
            (
                Arc::new(Field::new_list(
                    "pos_list",
                    Arc::new(Field::new("item", DataType::Int32, true)),
                    true,
                )),
                Arc::new(pos_list_builder.finish()) as ArrayRef,
            ),
        ]);
        println!("{:?}", struct_array);

        // Example of how to use the struct array, which is one value for a term
        let mut blockfile = HashMapBlockfile::open("test").unwrap();
        let key = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: "term1".to_string(),
        };
        let _res = blockfile.set(key.clone(), struct_array).unwrap();
        let posting_list = blockfile.get(key).unwrap();
        println!("{:?}", posting_list);

        let ids = posting_list.column(0);
        let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
        // find index of target id
        let target_id = 2;

        // imagine this is binary search instead of linear
        let mut found = false;
        for i in 0..ids.len() {
            if ids.is_null(i) {
                continue;
            }
            if ids.value(i) == target_id {
                found = true;
                let pos_list = posting_list.column(1);
                let pos_list = pos_list
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(i);
                println!(
                    "Found position list: {:?} for target id: {}",
                    pos_list, target_id
                );
                break;
            }
        }
    }
}
