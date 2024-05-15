use crate::blockstore::arrow::types::{ArrowReadableKey, ArrowWriteableKey};
use crate::blockstore::memory::storage::Writeable;
use crate::blockstore::{key::KeyWrapper, BlockfileFlusher, BlockfileReader, BlockfileWriter};
use crate::errors::ChromaError;

use core::ops::BitOr;
use roaring::RoaringBitmap;
use std::{collections::HashMap, marker::PhantomData};
use uuid::Uuid;

pub(crate) struct MetadataIndexWriter {
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

    pub fn set(
        &mut self,
        prefix: &str,
        key: K,
        offset_id: u32,
    ) -> Result<(), Box<dyn ChromaError>> {
        self.look_up_key_and_populate_uncommitted_rbms(prefix, &key)?;
        let rbms = self.uncommitted_rbms.get_mut(prefix).unwrap();
        let (_, rbm) = rbms.iter_mut().find(|(k, _)| k == &key).unwrap();
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

    pub fn commit(self) -> Result<MetadataIndexFlusher<K>, Box<dyn ChromaError>> {
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

    pub fn id(&self) -> Uuid {
        self.flusher.id()
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
        prefix: &str,
        key: K,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        let rbm = self.blockfile_reader.get(prefix, key).await;
        match rbm {
            Ok(rbm) => Ok(rbm),
            Err(e) => Err(e),
        }
    }

    pub async fn lt(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        let res = self.blockfile_reader.get_lt(prefix, key);
        match res {
            Ok(rbm) => {
                let mut result = RoaringBitmap::new();
                for (_, _, rbm) in rbm {
                    result = result.bitor(&rbm);
                }
                Ok(result)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn lte(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        let res = self.blockfile_reader.get_lte(prefix, key);
        match res {
            Ok(rbm) => {
                let mut result = RoaringBitmap::new();
                for (_, _, rbm) in rbm {
                    result = result.bitor(&rbm);
                }
                Ok(result)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn gt(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        let res = self.blockfile_reader.get_gt(prefix, key);
        match res {
            Ok(rbm) => {
                let mut result = RoaringBitmap::new();
                for (_, _, rbm) in rbm {
                    result = result.bitor(&rbm);
                }
                Ok(result)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn gte(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        let res = self.blockfile_reader.get_gte(prefix, key);
        match res {
            Ok(rbm) => {
                let mut result = RoaringBitmap::new();
                for (_, _, rbm) in rbm {
                    result = result.bitor(&rbm);
                }
                Ok(result)
            }
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
        let blockfile_writer = provider.create::<&str, &RoaringBitmap>().unwrap();
        let _writer = MetadataIndexWriter::<&str>::new(blockfile_writer);
    }

    #[tokio::test]
    async fn test_new_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let md_writer = MetadataIndexWriter::<&str>::new(blockfile_writer);
        let flusher = md_writer.commit().unwrap();
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
        let blockfile_writer = provider.create::<&str, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<&str>::new(blockfile_writer);
        writer.set("key", "value", 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
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
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<u32>::new(blockfile_writer);
        writer.set("key", 1, 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
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

    #[tokio::test]
    async fn test_float_metadata_index_set_get() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<f32>::new(blockfile_writer);
        writer.set("key", 1.0, 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<f32>::new(blockfile_reader);
        let bitmap = reader.get("key", 1.0).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[tokio::test]
    async fn test_bool_value_metadata_index_set_get() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<bool>::new(blockfile_writer);
        writer.set("key", true, 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<bool, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<bool>::new(blockfile_reader);
        let bitmap = reader.get("key", true).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.contains(1), true);
    }

    #[tokio::test]
    async fn test_string_value_metadata_multiple_keys() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<&str>::new(blockfile_writer);
        writer.set("key1", "value", 1).unwrap();
        writer.set("key1", "value", 2).unwrap();
        writer.set("key2", "value", 3).unwrap();
        writer.set("key2", "value2", 4).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<&str, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<&str>::new(blockfile_reader);
        let bitmap = reader.get("key1", "value").await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.get("key2", "value").await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(3));
    }

    #[tokio::test]
    async fn test_bool_value_metadata_multiple_keys() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<bool, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<bool>::new(blockfile_writer);
        writer.set("key1", true, 1).unwrap();
        writer.set("key1", true, 2).unwrap();
        writer.set("key2", true, 3).unwrap();
        writer.set("key2", false, 4).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<bool, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<bool>::new(blockfile_reader);
        let bitmap = reader.get("key1", true).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.get("key2", true).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(3));
    }

    #[tokio::test]
    async fn test_u32_metadata_multiple_keys() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<u32>::new(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 1, 2).unwrap();
        writer.set("key2", 1, 3).unwrap();
        writer.set("key2", 2, 4).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<u32>::new(blockfile_reader);
        let bitmap = reader.get("key1", 1).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.get("key2", 1).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(3));
    }

    #[tokio::test]
    async fn test_f32_value_metadata_multiple_keys() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<f32>::new(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 1.0, 2).unwrap();
        writer.set("key2", 1.0, 3).unwrap();
        writer.set("key2", 2.0, 4).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<f32>::new(blockfile_reader);
        let bitmap = reader.get("key1", 1.0).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.get("key2", 1.0).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(3));
    }

    #[tokio::test]
    async fn test_u32_value_metadata_lt_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<u32>::new(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 2, 2).unwrap();
        writer.set("key1", 3, 3).unwrap();
        writer.set("key1", 4, 4).unwrap();
        writer.set("key2", 5, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<u32>::new(blockfile_reader);
        let bitmap = reader.lt("key1", 3).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.lt("key2", 6).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.lt("key2", 5).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_u32_value_metadata_lte_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<u32>::new(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 2, 2).unwrap();
        writer.set("key1", 3, 3).unwrap();
        writer.set("key1", 4, 4).unwrap();
        writer.set("key2", 5, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<u32>::new(blockfile_reader);
        let bitmap = reader.lte("key1", 3).await.unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));

        let bitmap = reader.lte("key2", 5).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.lte("key2", 4).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_u32_value_metadata_gt_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<u32>::new(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 2, 2).unwrap();
        writer.set("key1", 3, 3).unwrap();
        writer.set("key1", 4, 4).unwrap();
        writer.set("key2", 5, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<u32>::new(blockfile_reader);
        let bitmap = reader.gt("key1", 2).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.gt("key2", 4).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.gt("key2", 5).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_u32_value_metadata_gte_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<u32>::new(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 2, 2).unwrap();
        writer.set("key1", 3, 3).unwrap();
        writer.set("key1", 4, 4).unwrap();
        writer.set("key2", 5, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<u32>::new(blockfile_reader);
        let bitmap = reader.gte("key1", 2).await.unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.gte("key2", 5).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.gte("key2", 6).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_f32_value_metadata_lt_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<f32>::new(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 2.0, 2).unwrap();
        writer.set("key1", 3.0, 3).unwrap();
        writer.set("key1", 4.0, 4).unwrap();
        writer.set("key2", 5.0, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<f32>::new(blockfile_reader);
        let bitmap = reader.lt("key1", 3.5).await.unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));

        let bitmap = reader.lt("key2", 6.0).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.lt("key2", 5.0).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_f32_value_metadata_lte_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<f32>::new(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 2.0, 2).unwrap();
        writer.set("key1", 3.0, 3).unwrap();
        writer.set("key1", 4.0, 4).unwrap();
        writer.set("key2", 5.0, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<f32>::new(blockfile_reader);
        let bitmap = reader.lte("key1", 4.0).await.unwrap();
        assert_eq!(bitmap.len(), 4);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.lte("key2", 5.0).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.lte("key2", 4.9).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_f32_value_metadata_gt_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<f32>::new(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 2.0, 2).unwrap();
        writer.set("key1", 3.0, 3).unwrap();
        writer.set("key1", 4.0, 4).unwrap();
        writer.set("key2", 5.0, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<f32>::new(blockfile_reader);
        let bitmap = reader.gt("key1", 2.0).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.gt("key2", 4.9).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.gt("key2", 5.0).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_f32_value_metadata_gte_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::<f32>::new(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 2.0, 2).unwrap();
        writer.set("key1", 3.0, 3).unwrap();
        writer.set("key1", 4.0, 4).unwrap();
        writer.set("key2", 5.0, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::<f32>::new(blockfile_reader);
        let bitmap = reader.gte("key1", 2.0).await.unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.gte("key2", 5.0).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.gte("key2", 5.1).await;
        assert!(bitmap.is_err());
    }
}
