use crate::blockstore::arrow::types::{ArrowReadableKey, ArrowWriteableKey};
use crate::blockstore::memory::storage::Writeable;
use crate::blockstore::{key::KeyWrapper, BlockfileFlusher, BlockfileReader, BlockfileWriter};
use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

use core::ops::BitOr;
use roaring::RoaringBitmap;
use std::{collections::HashMap, marker::PhantomData};

#[derive(Debug, Error)]
pub(crate) enum MetadataIndexError {
    #[error("Invalid key type")]
    InvalidKeyType,
}

impl ChromaError for MetadataIndexError {
    fn code(&self) -> crate::errors::ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

// This pattern for enum dispatch is weird. We do it for cause:
// - We can't incrementally write rbms to the blockfile -- we have to build up
//   each rbm then write them all at once.
//   - (We actually could incrementally write, but we would still need to track
//      intermediate state since blockfilewriters don't have read-then-write semantics.)
// - We can't store the rbms in a generic KeyWrapper -> rbm hashmap since KeyWrapper
//   doesn't implement Hash or Eq. We could implement them but the f32 type makes
//   that a little hairy.
// - We could do the Arrow pattern of having keys know how to write themselves
//  into MetadataIndexWriter store and long term we probably want to. But for now
//  this gets the job done.
pub(crate) enum MetadataIndexWriter {
    StringMetadataIndexWriter(
        BlockfileWriter,
        HashMap<String, HashMap<String, RoaringBitmap>>,
    ),
    U32MetadataIndexWriter(
        BlockfileWriter,
        HashMap<String, HashMap<u32, RoaringBitmap>>,
    ),
    // We use a Vec<(KeyWrapper, RoaringBitmap)> instead of a HashMap because
    // f32 doesn't implement Eq or Hash. Eq is trivial since we disallow
    // about NaN values, but Hash is harder.
    // Linear scanning is fine since we will only ever have 2^16 values
    // and the expected case is much less than that.
    F32MetadataIndexWriter(BlockfileWriter, HashMap<String, Vec<(f32, RoaringBitmap)>>),
    BoolMetadataIndexWriter(
        BlockfileWriter,
        HashMap<String, HashMap<bool, RoaringBitmap>>,
    ),
}

impl MetadataIndexWriter {
    pub fn new_string(init_blockfile_writer: BlockfileWriter) -> Self {
        MetadataIndexWriter::StringMetadataIndexWriter(init_blockfile_writer, HashMap::new())
    }

    pub fn new_u32(init_blockfile_writer: BlockfileWriter) -> Self {
        MetadataIndexWriter::U32MetadataIndexWriter(init_blockfile_writer, HashMap::new())
    }

    pub fn new_f32(init_blockfile_writer: BlockfileWriter) -> Self {
        MetadataIndexWriter::F32MetadataIndexWriter(init_blockfile_writer, HashMap::new())
    }

    pub fn new_bool(init_blockfile_writer: BlockfileWriter) -> Self {
        MetadataIndexWriter::BoolMetadataIndexWriter(init_blockfile_writer, HashMap::new())
    }

    fn look_up_key_and_populate_uncommitted_rbms(
        &mut self,
        prefix: &str,
        key: &KeyWrapper,
    ) -> Result<(), Box<dyn ChromaError>> {
        match self {
            MetadataIndexWriter::StringMetadataIndexWriter(_, uncommitted_rbms) => match key {
                KeyWrapper::String(k) => {
                    if !uncommitted_rbms.contains_key(prefix) {
                        uncommitted_rbms.insert(prefix.to_string(), HashMap::new());
                    }
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    if !rbms.contains_key(k) {
                        rbms.insert(k.to_string(), RoaringBitmap::new());
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexWriter::U32MetadataIndexWriter(_, uncommitted_rbms) => match key {
                KeyWrapper::Uint32(k) => {
                    if !uncommitted_rbms.contains_key(prefix) {
                        uncommitted_rbms.insert(prefix.to_string(), HashMap::new());
                    }
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    if !rbms.contains_key(k) {
                        rbms.insert(*k, RoaringBitmap::new());
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexWriter::F32MetadataIndexWriter(_, uncommitted_rbms) => match key {
                KeyWrapper::Float32(k) => {
                    if !uncommitted_rbms.contains_key(prefix) {
                        uncommitted_rbms.insert(prefix.to_string(), vec![]);
                    }
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    if !rbms.iter().any(|(rbm_k, _)| rbm_k == k) {
                        rbms.push((*k, RoaringBitmap::new()));
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexWriter::BoolMetadataIndexWriter(_, uncommitted_rbms) => match key {
                KeyWrapper::Bool(k) => {
                    if !uncommitted_rbms.contains_key(prefix) {
                        uncommitted_rbms.insert(prefix.to_string(), HashMap::new());
                    }
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    if !rbms.contains_key(k) {
                        rbms.insert(*k, RoaringBitmap::new());
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
        }
        Ok(())
    }

    pub fn set<K: Into<KeyWrapper>>(
        &mut self,
        prefix: &str,
        key: K,
        offset_id: u32,
    ) -> Result<(), Box<dyn ChromaError>> {
        let key = key.into();
        self.look_up_key_and_populate_uncommitted_rbms(prefix, &key)?;
        match self {
            MetadataIndexWriter::StringMetadataIndexWriter(_, uncommitted_rbms) => match key {
                KeyWrapper::String(k) => {
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.get_mut(&k).unwrap();
                    rbm.insert(offset_id);
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexWriter::BoolMetadataIndexWriter(_, uncommitted_rbms) => match key {
                KeyWrapper::Bool(k) => {
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.get_mut(&k).unwrap();
                    rbm.insert(offset_id);
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexWriter::U32MetadataIndexWriter(_, uncommitted_rbms) => match key {
                KeyWrapper::Uint32(k) => {
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.get_mut(&k).unwrap();
                    rbm.insert(offset_id);
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexWriter::F32MetadataIndexWriter(_, uncommitted_rbms) => match key {
                KeyWrapper::Float32(k) => {
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.iter_mut().find(|(rbm_k, _)| *rbm_k == k).unwrap();
                    rbm.1.insert(offset_id);
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
        }
        Ok(())
    }

    pub async fn write_to_blockfile(&mut self) -> Result<(), Box<dyn ChromaError>> {
        match self {
            MetadataIndexWriter::StringMetadataIndexWriter(blockfile_writer, uncommitted_rbms) => {
                for (prefix, rbms) in uncommitted_rbms.drain() {
                    for (key, rbm) in rbms.iter() {
                        blockfile_writer
                            .set(prefix.as_str(), key.as_str(), rbm)
                            .await?
                    }
                }
            }
            MetadataIndexWriter::U32MetadataIndexWriter(blockfile_writer, uncommitted_rbms) => {
                for (prefix, rbms) in uncommitted_rbms.drain() {
                    for (key, rbm) in rbms.iter() {
                        blockfile_writer.set(prefix.as_str(), *key, rbm).await?
                    }
                }
            }
            MetadataIndexWriter::F32MetadataIndexWriter(blockfile_writer, uncommitted_rbms) => {
                for (prefix, rbms) in uncommitted_rbms.drain() {
                    for (key, rbm) in rbms.iter() {
                        blockfile_writer.set(prefix.as_str(), *key, rbm).await?
                    }
                }
            }
            MetadataIndexWriter::BoolMetadataIndexWriter(blockfile_writer, uncommitted_rbms) => {
                for (prefix, rbms) in uncommitted_rbms.drain() {
                    for (key, rbm) in rbms.iter() {
                        blockfile_writer.set(prefix.as_str(), *key, rbm).await?
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn commit(self) -> Result<MetadataIndexFlusher, Box<dyn ChromaError>> {
        match self {
            MetadataIndexWriter::StringMetadataIndexWriter(blockfile_writer, _) => {
                Ok(MetadataIndexFlusher::StringMetadataIndexFlusher(
                    blockfile_writer.commit::<&str, &RoaringBitmap>()?,
                ))
            }
            MetadataIndexWriter::U32MetadataIndexWriter(blockfile_writer, _) => {
                Ok(MetadataIndexFlusher::U32MetadataIndexFlusher(
                    blockfile_writer.commit::<u32, &RoaringBitmap>()?,
                ))
            }
            MetadataIndexWriter::F32MetadataIndexWriter(blockfile_writer, _) => {
                Ok(MetadataIndexFlusher::F32MetadataIndexFlusher(
                    blockfile_writer.commit::<f32, &RoaringBitmap>()?,
                ))
            }
            MetadataIndexWriter::BoolMetadataIndexWriter(blockfile_writer, _) => {
                Ok(MetadataIndexFlusher::BoolMetadataIndexFlusher(
                    blockfile_writer.commit::<bool, &RoaringBitmap>()?,
                ))
            }
        }
    }
}

pub(crate) enum MetadataIndexFlusher {
    StringMetadataIndexFlusher(BlockfileFlusher),
    U32MetadataIndexFlusher(BlockfileFlusher),
    F32MetadataIndexFlusher(BlockfileFlusher),
    BoolMetadataIndexFlusher(BlockfileFlusher),
}

impl MetadataIndexFlusher {
    pub async fn flush(self) -> Result<(), Box<dyn ChromaError>> {
        match self {
            MetadataIndexFlusher::StringMetadataIndexFlusher(flusher) => {
                flusher.flush::<&str, &RoaringBitmap>().await
            }
            MetadataIndexFlusher::U32MetadataIndexFlusher(flusher) => {
                flusher.flush::<u32, &RoaringBitmap>().await
            }
            MetadataIndexFlusher::F32MetadataIndexFlusher(flusher) => {
                flusher.flush::<f32, &RoaringBitmap>().await
            }
            MetadataIndexFlusher::BoolMetadataIndexFlusher(flusher) => {
                flusher.flush::<bool, &RoaringBitmap>().await
            }
        }
    }
}

pub(crate) enum MetadataIndexReader<'me> {
    StringMetadataIndexReader(BlockfileReader<'me, &'me str, RoaringBitmap>),
    U32MetadataIndexReader(BlockfileReader<'me, u32, RoaringBitmap>),
    F32MetadataIndexReader(BlockfileReader<'me, f32, RoaringBitmap>),
    BoolMetadataIndexReader(BlockfileReader<'me, bool, RoaringBitmap>),
}

impl<'me> MetadataIndexReader<'me> {
    pub fn new_string(
        init_blockfile_reader: BlockfileReader<'me, &'me str, RoaringBitmap>,
    ) -> Self {
        MetadataIndexReader::StringMetadataIndexReader(init_blockfile_reader)
    }

    pub fn new_u32(init_blockfile_reader: BlockfileReader<'me, u32, RoaringBitmap>) -> Self {
        MetadataIndexReader::U32MetadataIndexReader(init_blockfile_reader)
    }

    pub fn new_f32(init_blockfile_reader: BlockfileReader<'me, f32, RoaringBitmap>) -> Self {
        MetadataIndexReader::F32MetadataIndexReader(init_blockfile_reader)
    }

    pub fn new_bool(init_blockfile_reader: BlockfileReader<'me, bool, RoaringBitmap>) -> Self {
        MetadataIndexReader::BoolMetadataIndexReader(init_blockfile_reader)
    }

    pub async fn get(
        &'me self,
        prefix: &str,
        key: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        match self {
            MetadataIndexReader::StringMetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::String(k) => {
                    let rbm = blockfile_reader.get(prefix, k).await;
                    match rbm {
                        Ok(rbm) => Ok(rbm),
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Uint32(k) => {
                    let rbm = blockfile_reader.get(prefix, *k).await;
                    match rbm {
                        Ok(rbm) => Ok(rbm),
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Float32(k) => {
                    let rbm = blockfile_reader.get(prefix, *k).await;
                    match rbm {
                        Ok(rbm) => Ok(rbm),
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexReader::BoolMetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Bool(k) => {
                    let rbm = blockfile_reader.get(prefix, *k).await;
                    match rbm {
                        Ok(rbm) => Ok(rbm),
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
        }
    }

    pub async fn lt(
        &'me self,
        prefix: &str,
        key: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        match self {
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Uint32(k) => {
                    let read = blockfile_reader.get_lt(prefix, *k);
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, _, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Float32(k) => {
                    let read = blockfile_reader.get_lt(prefix, *k);
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, _, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
        }
    }

    pub async fn lte(
        &'me self,
        prefix: &str,
        key: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        match self {
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Uint32(k) => {
                    let read = blockfile_reader.get_lte(prefix, *k);
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, _, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Float32(k) => {
                    let read = blockfile_reader.get_lt(prefix, *k);
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, _, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
        }
    }

    pub async fn gt(
        &'me self,
        prefix: &str,
        key: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        match self {
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Uint32(k) => {
                    let read = blockfile_reader.get_gt(prefix, *k);
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, _, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Float32(k) => {
                    let read = blockfile_reader.get_gt(prefix, *k);
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, _, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
        }
    }

    pub async fn gte(
        &'me self,
        prefix: &str,
        key: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, Box<dyn ChromaError>> {
        match self {
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Uint32(k) => {
                    let read = blockfile_reader.get_gte(prefix, *k);
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, _, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match key {
                KeyWrapper::Float32(k) => {
                    let read = blockfile_reader.get_gte(prefix, *k);
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, _, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
            },
            _ => return Err(Box::new(MetadataIndexError::InvalidKeyType)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::blockstore::provider::BlockfileProvider;

    #[test]
    fn test_new_string_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &RoaringBitmap>().unwrap();
        let _writer = MetadataIndexWriter::new_string(blockfile_writer);
    }

    #[test]
    fn test_new_u32_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let _writer = MetadataIndexWriter::new_u32(blockfile_writer);
    }

    #[test]
    fn test_new_f32_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let _writer = MetadataIndexWriter::new_f32(blockfile_writer);
    }

    #[test]
    fn test_new_bool_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<bool, &RoaringBitmap>().unwrap();
        let _writer = MetadataIndexWriter::new_bool(blockfile_writer);
    }

    #[tokio::test]
    async fn test_new_string_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut md_writer = MetadataIndexWriter::new_string(blockfile_writer);
        md_writer.write_to_blockfile().await.unwrap();
        let flusher = md_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<&str, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let _reader = MetadataIndexReader::new_string(blockfile_reader);
    }

    #[tokio::test]
    async fn test_new_u32_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut md_writer = MetadataIndexWriter::new_u32(blockfile_writer);
        md_writer.write_to_blockfile().await.unwrap();
        let flusher = md_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let _reader = MetadataIndexReader::new_u32(blockfile_reader);
    }

    #[tokio::test]
    async fn test_new_f32_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut md_writer = MetadataIndexWriter::new_f32(blockfile_writer);
        md_writer.write_to_blockfile().await.unwrap();
        let flusher = md_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let _reader = MetadataIndexReader::new_f32(blockfile_reader);
    }

    #[tokio::test]
    async fn test_new_bool_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<bool, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut md_writer = MetadataIndexWriter::new_bool(blockfile_writer);
        md_writer.write_to_blockfile().await.unwrap();
        let flusher = md_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<bool, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let _reader = MetadataIndexReader::new_bool(blockfile_reader);
    }

    #[tokio::test]
    async fn test_string_metadata_index_set_get() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_string(blockfile_writer);
        writer.set("key", "value", 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<&str, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_string(blockfile_reader);
        let bitmap = reader.get("key", &"value".into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(1));
    }

    #[tokio::test]
    async fn test_u32_metadata_index_set_get() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer);
        writer.set("key", 1, 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_u32(blockfile_reader);
        let bitmap = reader.get("key", &1.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(1));
    }

    #[tokio::test]
    async fn test_f32_metadata_index_set_get() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer);
        writer.set("key", 1.0, 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_f32(blockfile_reader);
        let bitmap = reader.get("key", &1.0.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(1));
    }

    #[tokio::test]
    async fn test_bool_value_metadata_index_set_get() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<bool, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_bool(blockfile_writer);
        writer.set("key", true, 1).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<bool, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_bool(blockfile_reader);
        let bitmap = reader.get("key", &true.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(1));
    }

    #[tokio::test]
    async fn test_string_metadata_multiple_keys() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_string(blockfile_writer);
        writer.set("key1", "value", 1).unwrap();
        writer.set("key1", "value", 2).unwrap();
        writer.set("key2", "value", 3).unwrap();
        writer.set("key2", "value2", 4).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<&str, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_string(blockfile_reader);
        let bitmap = reader.get("key1", &"value".into()).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.get("key2", &"value".into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(3));
    }

    #[tokio::test]
    async fn test_u32_metadata_multiple_keys() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 1, 2).unwrap();
        writer.set("key2", 1, 3).unwrap();
        writer.set("key2", 2, 4).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_u32(blockfile_reader);
        let bitmap = reader.get("key1", &1.into()).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.get("key2", &1.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(3));
    }

    #[tokio::test]
    async fn test_f32_metadata_multiple_keys() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 1.0, 2).unwrap();
        writer.set("key2", 1.0, 3).unwrap();
        writer.set("key2", 2.0, 4).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_f32(blockfile_reader);
        let bitmap = reader.get("key1", &1.0.into()).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.get("key2", &1.0.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(3));
    }

    #[tokio::test]
    async fn test_bool_metadata_multiple_keys() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<bool, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_bool(blockfile_writer);
        writer.set("key1", true, 1).unwrap();
        writer.set("key1", true, 2).unwrap();
        writer.set("key2", true, 3).unwrap();
        writer.set("key2", false, 4).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<bool, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_bool(blockfile_reader);
        let bitmap = reader.get("key1", &true.into()).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.get("key2", &true.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(3));
    }

    #[tokio::test]
    async fn test_u32_metadata_lt_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 2, 2).unwrap();
        writer.set("key1", 3, 3).unwrap();
        writer.set("key1", 4, 4).unwrap();
        writer.set("key2", 5, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_u32(blockfile_reader);
        let bitmap = reader.lt("key1", &3.into()).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));

        let bitmap = reader.lt("key2", &6.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.lt("key2", &5.into()).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_u32_value_metadata_lte_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 2, 2).unwrap();
        writer.set("key1", 3, 3).unwrap();
        writer.set("key1", 4, 4).unwrap();
        writer.set("key2", 5, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_u32(blockfile_reader);
        let bitmap = reader.lte("key1", &3.into()).await.unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));

        let bitmap = reader.lte("key2", &5.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.lte("key2", &4.into()).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_u32_value_metadata_gt_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 2, 2).unwrap();
        writer.set("key1", 3, 3).unwrap();
        writer.set("key1", 4, 4).unwrap();
        writer.set("key2", 5, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_u32(blockfile_reader);
        let bitmap = reader.gt("key1", &2.into()).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.gt("key2", &4.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.gt("key2", &5.into()).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_u32_value_metadata_gte_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer);
        writer.set("key1", 1, 1).unwrap();
        writer.set("key1", 2, 2).unwrap();
        writer.set("key1", 3, 3).unwrap();
        writer.set("key1", 4, 4).unwrap();
        writer.set("key2", 5, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<u32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_u32(blockfile_reader);
        let bitmap = reader.gte("key1", &2.into()).await.unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.gte("key2", &5.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.gte("key2", &6.into()).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_f32_metadata_lt_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 2.0, 2).unwrap();
        writer.set("key1", 3.0, 3).unwrap();
        writer.set("key1", 4.0, 4).unwrap();
        writer.set("key2", 5.0, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_f32(blockfile_reader);
        let bitmap = reader.lt("key1", &3.5.into()).await.unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));

        let bitmap = reader.lt("key2", &6.0.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.lt("key2", &5.0.into()).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_f32_metadata_lte_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 2.0, 2).unwrap();
        writer.set("key1", 3.0, 3).unwrap();
        writer.set("key1", 4.0, 4).unwrap();
        writer.set("key2", 5.0, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_f32(blockfile_reader);
        let bitmap = reader.lte("key1", &4.00001.into()).await.unwrap();
        assert_eq!(bitmap.len(), 4);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.lte("key2", &5.00001.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.lte("key2", &4.9.into()).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_f32_metadata_gt_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 2.0, 2).unwrap();
        writer.set("key1", 3.0, 3).unwrap();
        writer.set("key1", 4.0, 4).unwrap();
        writer.set("key2", 5.0, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_f32(blockfile_reader);
        let bitmap = reader.gt("key1", &2.0.into()).await.unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.gt("key2", &4.0.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.gt("key2", &5.0.into()).await;
        assert!(bitmap.is_err());
    }

    #[tokio::test]
    async fn test_f32_metadata_gte_operator() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, &RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();

        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer);
        writer.set("key1", 1.0, 1).unwrap();
        writer.set("key1", 2.0, 2).unwrap();
        writer.set("key1", 3.0, 3).unwrap();
        writer.set("key1", 4.0, 4).unwrap();
        writer.set("key2", 5.0, 5).unwrap();
        writer.write_to_blockfile().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let blockfile_reader = provider
            .open::<f32, RoaringBitmap>(&writer_id)
            .await
            .unwrap();
        let reader = MetadataIndexReader::new_f32(blockfile_reader);
        let bitmap = reader.gte("key1", &2.0.into()).await.unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(2));
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));

        let bitmap = reader.gte("key2", &5.0.into()).await.unwrap();
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(5));

        let bitmap = reader.gte("key2", &6.0.into()).await;
        assert!(bitmap.is_err());
    }
}
