use crate::errors::ChromaError;
use arrow::array::{
    Array, ArrayRef, Int32Array, Int32Builder, ListArray, ListBuilder, StructArray, StructBuilder,
};
use arrow::datatypes::{DataType, Field};
use std::fmt::Display;
use std::hash::{Hash, Hasher};

use super::values::{PositionalPostingList, PositionalPostingListBuilder};

#[derive(Clone)]
pub(crate) struct BlockfileKey {
    pub(crate) prefix: String,
    pub(crate) key: Key,
}

#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub(crate) enum Key {
    String(String),
    Float(f32),
}

#[derive(Debug)]
pub(crate) enum Value {
    Int32ArrayValue(Int32Array),
    PositionalPostingListValue(PositionalPostingList),
    Int32(i32),
    String(String),
    // Future values can be the struct type for positional inverted indices and
    // the roaring bitmap for doc ids
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::String(s) => write!(f, "{}", s),
            Key::Float(fl) => write!(f, "{}", fl),
        }
    }
}

impl BlockfileKey {
    pub(crate) fn new(prefix: String, key: Key) -> Self {
        BlockfileKey { prefix, key }
    }
}

impl Hash for BlockfileKey {
    // Hash is only used for the HashMap implementation, which is a test/reference implementation
    // Therefore this hash implementation is not used in production and allowed to be
    // hacky
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prefix.hash(state);
    }
}

impl PartialEq for BlockfileKey {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.key == other.key
    }
}

impl PartialOrd for BlockfileKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.prefix == other.prefix {
            self.key.partial_cmp(&other.key)
        } else {
            self.prefix.partial_cmp(&other.prefix)
        }
    }
}

impl Eq for BlockfileKey {}

// TODO: align with rust collection conventions for this trait
pub(crate) trait SplittableBlockFileValue {
    fn get_at_index(&self, index: usize) -> Result<&Value, Box<dyn ChromaError>>;
    fn len(&self) -> usize;
}

pub(crate) trait Blockfile {
    // TODO: check the into string pattern
    fn open(path: &str) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;
    fn get(&self, key: BlockfileKey) -> Result<&Value, Box<dyn ChromaError>>;
    fn get_by_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>>;

    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;

    fn set(&mut self, key: BlockfileKey, value: Value) -> Result<(), Box<dyn ChromaError>>;

    // TODO: the naming of these methods are off since they don't mention the prefix
    // THOUGHT: make prefix optional and if its included, then it will be used to filter the results
    // Get all values with a given prefix where the key is greater than the given key
    fn get_gt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>>;

    // Get all values with a given prefix where the key is less than the given key
    fn get_lt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>>;

    // Get all values with a given prefix where the key is greater than or equal to the given key
    fn get_gte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>>;

    fn get_lte(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>>;
}

pub(crate) trait SplittableBlockFile<V: SplittableBlockFileValue>: Blockfile {
    fn get_with_value_hint(
        &self,
        key: BlockfileKey,
        value_hint: Value,
    ) -> Result<&Value, Box<dyn ChromaError>>;
}

pub(crate) struct HashMapBlockfile {
    map: std::collections::HashMap<BlockfileKey, Value>,
}

impl Blockfile for HashMapBlockfile {
    // TODO: change this to respect path instead of ignoring it and creating a new thing
    fn open(_path: &str) -> Result<Self, Box<dyn ChromaError>> {
        Ok(HashMapBlockfile {
            map: std::collections::HashMap::new(),
        })
    }
    fn get(&self, key: BlockfileKey) -> Result<&Value, Box<dyn ChromaError>> {
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
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>> {
        let mut result = Vec::new();
        for (key, value) in self.map.iter() {
            if key.prefix == prefix {
                result.push((key, value));
            }
        }
        Ok(result)
    }

    fn set(&mut self, key: BlockfileKey, value: Value) -> Result<(), Box<dyn ChromaError>> {
        self.map.insert(key, value);
        Ok(())
    }

    fn get_gt(
        &self,
        prefix: String,
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>> {
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
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>> {
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
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>> {
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
        key: Key,
    ) -> Result<Vec<(&BlockfileKey, &Value)>, Box<dyn ChromaError>> {
        let mut result = Vec::new();
        for (k, v) in self.map.iter() {
            if k.prefix == prefix && k.key <= key {
                result.push((k, v));
            }
        }
        Ok(result)
    }

    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, sync::Arc};

    use k8s_openapi::List;
    use prost_types::Struct;

    use super::*;

    impl Debug for BlockfileKey {
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
            key: Key::String("key1".to_string()),
        };
        let _res = blockfile
            .set(key.clone(), "value1".to_string().into())
            .unwrap();
        let value = blockfile.get(key);
        // downcast to string
        match value.unwrap() {
            Value::String(s) => assert_eq!(s, "value1"),
            _ => panic!("Value is not a string"),
        }
    }

    #[test]
    fn test_blockfile_get_by_prefix() {
        let mut blockfile = HashMapBlockfile::open("test").unwrap();
        let key1 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key1".to_string()),
        };
        let key2 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key2".to_string()),
        };
        let _res = blockfile
            .set(key1.clone(), "value1".to_string().into())
            .unwrap();
        let _res = blockfile
            .set(key2.clone(), "value2".to_string().into())
            .unwrap();
        let values = blockfile.get_by_prefix("text_prefix".to_string()).unwrap();
        assert_eq!(values.len(), 2);
        // May return values in any order
        match values[0].1 {
            Value::String(s) => assert!(s == "value1" || s == "value2"),
            _ => panic!("Value is not a string"),
        }
        match values[1].1 {
            Value::String(s) => assert!(s == "value1" || s == "value2"),
            _ => panic!("Value is not a string"),
        }
    }

    #[test]
    fn test_storing_arrow_in_blockfile() {
        let mut blockfile = HashMapBlockfile::open("test").unwrap();
        let key = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key1".to_string()),
        };
        let array = Value::Int32ArrayValue(Int32Array::from(vec![1, 2, 3]));
        let _res = blockfile.set(key.clone(), array).unwrap();
        let value = blockfile.get(key).unwrap();
        match value {
            Value::Int32ArrayValue(arr) => assert_eq!(arr, &Int32Array::from(vec![1, 2, 3])),
            _ => panic!("Value is not an arrow int32 array"),
        }
    }

    #[test]
    fn test_blockfile_get_gt() {
        let mut blockfile = HashMapBlockfile::open("test").unwrap();
        let key1 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key1".to_string()),
        };
        let key2 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key2".to_string()),
        };
        let key3 = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("key3".to_string()),
        };
        let _res = blockfile.set(key1.clone(), Value::Int32(1)).unwrap();
        let _res = blockfile.set(key2.clone(), Value::Int32(2)).unwrap();
        let _res = blockfile.set(key3.clone(), Value::Int32(3)).unwrap();
        let values = blockfile
            .get_gt("text_prefix".to_string(), Key::String("key1".to_string()))
            .unwrap();
        assert_eq!(values.len(), 2);
        match &values[0].0.key {
            Key::String(s) => assert!(s == "key2" || s == "key3"),
            _ => panic!("Key is not a string"),
        }
        match &values[1].0.key {
            Key::String(s) => assert!(s == "key2" || s == "key3"),
            _ => panic!("Key is not a string"),
        }
    }

    #[test]
    fn test_learning_arrow_struct() {
        // positional inverted index is term -> doc_ids -> positions
        // lets construct ["term1", "term2"] -> [[1, 2, 3], [4]] -> [[[0], [0, 1], [0, 1, 2]], [[10]]]
        // this is implemented as two KV
        // term1 -> Struct(ids: [1, 2, 3], pos: [[0], [0, 1], [0, 1, 2]])
        // term2 -> Struct(ids: [4], pos: [[10]])
        // let mut id_list_builder = Int32Builder::new();
        // id_list_builder.append_value(1);
        // id_list_builder.append_value(2);
        // id_list_builder.append_value(3);
        // let id_list = id_list_builder.finish();

        // let mut pos_list_builder = ListBuilder::new(Int32Builder::new());
        // // Create the first list [[0], [0, 1], [0, 1, 2]]
        // let term1 = pos_list_builder.values();
        // term1.append_value(0);
        // pos_list_builder.append(true);
        // let term1 = pos_list_builder.values();
        // term1.append_value(0);
        // term1.append_value(1);
        // pos_list_builder.append(true);
        // let term1 = pos_list_builder.values();
        // term1.append_value(0);
        // term1.append_value(1);
        // term1.append_value(2);
        // pos_list_builder.append(true);

        // // TODO: build the ids such that they don't have to be named "item" and be nullable
        // let struct_array = StructArray::from(vec![
        //     (
        //         Arc::new(Field::new("id_list", DataType::Int32, true)),
        //         Arc::new(id_list.clone()) as ArrayRef,
        //     ),
        //     (
        //         Arc::new(Field::new_list(
        //             "pos_list",
        //             Arc::new(Field::new("item", DataType::Int32, true)),
        //             true,
        //         )),
        //         Arc::new(pos_list_builder.finish()) as ArrayRef,
        //     ),
        // ]);
        // println!("{:?}", struct_array);
        let mut builder = PositionalPostingListBuilder::new();
        builder.add_doc_id_and_positions(1, vec![0]);
        builder.add_doc_id_and_positions(2, vec![0, 1]);
        builder.add_doc_id_and_positions(3, vec![0, 1, 2]);
        let list_term_1 = builder.build();

        // Example of how to use the struct array, which is one value for a term
        let mut blockfile = HashMapBlockfile::open("test").unwrap();
        let key = BlockfileKey {
            prefix: "text_prefix".to_string(),
            key: Key::String("term1".to_string()),
        };
        let _res = blockfile
            .set(key.clone(), Value::PositionalPostingListValue(list_term_1))
            .unwrap();
        let posting_list = blockfile.get(key).unwrap();
        let posting_list = match posting_list {
            Value::PositionalPostingListValue(arr) => arr,
            _ => panic!("Value is not an arrow struct array"),
        };
        println!("{:?}", posting_list);

        let ids = posting_list.get_doc_ids();
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
                let pos_list = posting_list.get_positions_for_doc_id(target_id).unwrap();
                println!(
                    "Found position list: {:?} for target id: {}",
                    pos_list, target_id
                );
                break;
            }
        }
    }
}
