use crate::blockstore::arrow::types::{ArrowReadableKey, ArrowWriteableKey};
use crate::blockstore::memory::storage::Writeable;
use crate::blockstore::{key::KeyWrapper, BlockfileFlusher, BlockfileReader, BlockfileWriter};
use crate::errors::ChromaError;

use roaring::RoaringBitmap;
use std::{collections::HashMap, marker::PhantomData};

pub(crate) struct MetadataIndexWriter<K: ArrowWriteableKey + Writeable> {
    blockfile_writer: BlockfileWriter,
    // We use a Vec<(KeyWrapper, RoaringBitmap)> instead of a HashMap because
    // f32 doesn't implement Eq or Hash. Eq is trivial since we disallow
    // about NaN values, but Hash is harder.
    // Linear scanning is fine since we will only ever have 2^16 values
    // and the expected case is much less than that.
    uncommitted_rbms: HashMap<String, Vec<(K, RoaringBitmap)>>,
}

impl<K: ArrowWriteableKey + Writeable> MetadataIndexWriter<K> {
    pub fn new(init_blockfile_writer: BlockfileWriter) -> Self {
        MetadataIndexWriter {
            blockfile_writer: init_blockfile_writer,
            uncommitted_rbms: HashMap::new(),
        }
    }

    fn look_up_key_and_populate_uncommitted_rbms(
        &mut self,
        prefix: &str,
        key: &K,
    ) -> Result<(), Box<dyn ChromaError>> {
        if !self.uncommitted_rbms.contains_key(prefix) {
            self.uncommitted_rbms.insert(prefix.to_string(), vec![]);
        }
        let rbms = self.uncommitted_rbms.get_mut(prefix).unwrap();
        if !rbms.iter().any(|(k, _)| k == key) {
            rbms.push((key.clone(), RoaringBitmap::new()));
        }
        Ok(())
    }

    pub fn set(&mut self, key: &str, value: K, offset_id: u32) -> Result<(), Box<dyn ChromaError>> {
        self.look_up_key_and_populate_uncommitted_rbms(key, &value)?;
        let rbms = self.uncommitted_rbms.get_mut(key).unwrap();
        let (_, rbm) = rbms.iter_mut().find(|(k, _)| k == &value).unwrap();
        rbm.insert(offset_id);
        Ok(())
    }

    pub async fn write_to_blockfile(&mut self) -> Result<(), Box<dyn ChromaError>> {
        for (key, mut rbms) in self.uncommitted_rbms.drain() {
            for (value, rbm) in rbms.drain(..) {
                self.blockfile_writer.set(key.as_str(), value, &rbm).await?;
            }
        }
        self.uncommitted_rbms.clear();
        Ok(())
    }

    pub async fn commit(self) -> Result<MetadataIndexFlusher<K>, Box<dyn ChromaError>> {
        let f = match self.blockfile_writer.commit::<K, &RoaringBitmap>() {
            Ok(f) => f,
            Err(e) => return Err(e),
        };
        Ok(MetadataIndexFlusher {
            phantom: PhantomData,
            flusher: f,
        })
    }
}

pub(crate) struct MetadataIndexFlusher<K: ArrowWriteableKey + Writeable> {
    phantom: PhantomData<K>,
    flusher: BlockfileFlusher,
}

impl<K: ArrowWriteableKey + Writeable> MetadataIndexFlusher<K> {
    pub async fn flush(self) -> Result<(), Box<dyn ChromaError>> {
        self.flusher.flush::<K, &RoaringBitmap>().await
    }
}

pub(crate) struct MetadataIndexReader<
    'me,
    K: Into<KeyWrapper> + From<&'me KeyWrapper> + ArrowReadableKey<'me> + Clone,
> {
    metadata_value_type: PhantomData<K>,
    blockfile_reader: BlockfileReader<'me, K, RoaringBitmap>,
}

impl<'me, K: Into<KeyWrapper> + From<&'me KeyWrapper> + ArrowReadableKey<'me> + Clone>
    MetadataIndexReader<'me, K>
{
    pub fn new(init_blockfile_reader: BlockfileReader<'me, K, RoaringBitmap>) -> Self {
        MetadataIndexReader {
            metadata_value_type: PhantomData,
            blockfile_reader: init_blockfile_reader,
        }
    }

    pub async fn get(
        &'me self,
        key: &str,
        value: K,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        let rbm = self.blockfile_reader.get(key, value).await;
        match rbm {
            Ok(rbm) => Ok(rbm),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::blockstore::provider::BlockfileProvider;

    #[test]
    fn test_new_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &str>().unwrap();
        let _writer = MetadataIndexWriter::<&str>::new(blockfile_writer);
    }

    #[tokio::test]
    async fn test_new_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &str>().unwrap();
        let writer_id = blockfile_writer.id();
        let md_writer = MetadataIndexWriter::<&str>::new(blockfile_writer);
        let flusher = md_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<&str, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let _reader = MetadataIndexReader::<&str>::new(blockfile_reader);
    }

    #[tokio::test]
    async fn test_string_metadata_index_set_get() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &str>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<&str>::new(blockfile_writer);
        writer.set("key", "value", 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<&str, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<&str>::new(blockfile_reader);
        let bitmap = reader.get("key", "value").await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[tokio::test]
    async fn test_u32_metadata_index_set_get() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &str>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<u32>::new(blockfile_writer);
        writer.set("key", 1, 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<u32>::new(blockfile_reader);
        let bitmap = reader.get("key", 1).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    // #[tokio::test]
    // async fn test_float_metadata_index_set_get() {
    //     let provider = BlockfileProvider::new_memory();
    //     let blockfile_writer = provider.create::<f32, &str>().unwrap();
    //     let writer_id = blockfile_writer.id();

    //     let mut writer = MetadataIndexWriter::<f32>::new(blockfile_writer);
    //     writer.set("key", 1.0, 1).unwrap();
    //     writer.write_to_blockfile().await.unwrap();
    //     let flusher = writer.commit().await.unwrap();
    //     flusher.flush().await.unwrap();

    //     let blockfile_reader = provider
    //         .open::<f32, RoaringBitmap>(&writer_id)
    //         .await
    //         .unwrap();
    //     let reader = MetadataIndexReader::<f32>::new(blockfile_reader);
    //     let bitmap = reader.get("key", 1.0).await.unwrap();
    //     assert_eq!(bitmap.len(), 1);
    //     assert_eq!(bitmap.contains(1), true);
    // }

    // #[tokio::test]
    // async fn test_bool_value_metadata_index_set_get() {
    //     let provider = BlockfileProvider::new_memory();
    //     let blockfile_writer = provider.create::<&str, &str>().unwrap();
    //     let writer_id = blockfile_writer.id();

    //     let mut writer = MetadataIndexWriter::<bool>::new(blockfile_writer);
    //     writer.set("key", true, 1).unwrap();
    //     writer.write_to_blockfile().await.unwrap();
    //     let flusher = writer.commit().await.unwrap();
    //     flusher.flush().await.unwrap();

    //     let blockfile_reader = provider
    //         .open::<&str, RoaringBitmap>(&writer_id)
    //         .await
    //         .unwrap();
    //     let reader = MetadataIndexReader::<&str>::new(blockfile_reader);
    //     let bitmap = reader.get("key", true).await.unwrap();
    //     assert_eq!(bitmap.len(), 1);
    //     assert_eq!(bitmap.contains(1), true);
    // }
}

//     #[test]
//     fn test_bool_value_metadata_index_set_get() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<bool>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", true, 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", true).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);
//     }

//     #[test]
//     fn test_string_value_metadata_index_set_delete_get() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", "value".to_string(), 1).unwrap();
//         index.delete("key", "value".to_string(), 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 0);
//     }

//     #[test]
//     fn test_string_value_metadata_index_set_delete_set_get() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", "value".to_string(), 1).unwrap();
//         index.delete("key", "value".to_string(), 1).unwrap();
//         index.set("key", "value".to_string(), 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);
//     }

//     #[test]
//     fn test_string_value_metadata_index_multiple_keys() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key1", "value".to_string(), 1).unwrap();
//         index.set("key2", "value".to_string(), 2).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key1", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);

//         let bitmap = index.get("key2", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(2), true);
//     }

//     #[test]
//     fn test_string_value_metadata_index_multiple_values() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", "value1".to_string(), 1).unwrap();
//         index.set("key", "value2".to_string(), 2).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", "value1".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(1), true);

//         let bitmap = index.get("key", "value2".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 1);
//         assert_eq!(bitmap.contains(2), true);
//     }

//     #[test]
//     fn test_string_value_metadata_index_delete_in_standalone_transaction() {
//         let mut provider = HashMapBlockfileProvider::new();
//         let blockfile = provider
//             .create("test", KeyType::String, ValueType::RoaringBitmap)
//             .unwrap();
//         let mut index = BlockfileMetadataIndex::<String>::new(blockfile);
//         index.begin_transaction().unwrap();
//         index.set("key", "value".to_string(), 1).unwrap();
//         index.commit_transaction().unwrap();

//         index.begin_transaction().unwrap();
//         index.delete("key", "value".to_string(), 1).unwrap();
//         index.commit_transaction().unwrap();

//         let bitmap = index.get("key", "value".to_string()).unwrap();
//         assert_eq!(bitmap.len(), 0);
//     }
