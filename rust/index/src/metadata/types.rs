use crate::fulltext::types::FullTextIndexError;
use chroma_blockstore::{key::KeyWrapper, BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use uuid::Uuid;

use core::ops::BitOr;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

#[derive(Debug, Error)]
pub enum MetadataIndexError {
    #[error("Invalid key type")]
    InvalidKeyType,
    #[error("Blockfile error: {0}")]
    BlockfileError(#[from] Box<dyn ChromaError>),
    #[error("Full text index error: {0}")]
    FullTextError(#[from] FullTextIndexError),
}

impl ChromaError for MetadataIndexError {
    fn code(&self) -> chroma_error::ErrorCodes {
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
#[derive(Clone)]
pub enum MetadataIndexWriter<'me> {
    // TODO(Sanket): Move off this tokio::sync::mutex and use
    // a lightweight lock instead. This is needed currently to
    // keep holding the lock across an await point.
    StringMetadataIndexWriter(
        BlockfileWriter,
        // We use this to implement updates which require read-then-write semantics.
        Option<MetadataIndexReader<'me>>,
        Arc<tokio::sync::Mutex<HashMap<String, HashMap<String, RoaringBitmap>>>>,
    ),
    U32MetadataIndexWriter(
        BlockfileWriter,
        // We use this to implement updates which require read-then-write semantics.
        Option<MetadataIndexReader<'me>>,
        Arc<tokio::sync::Mutex<HashMap<String, HashMap<u32, RoaringBitmap>>>>,
    ),
    // We use a Vec<(KeyWrapper, RoaringBitmap)> instead of a HashMap because
    // f32 doesn't implement Eq or Hash. Eq is trivial since we disallow
    // about NaN values, but Hash is harder.
    // Linear scanning is fine since we will only ever have 2^16 values
    // and the expected case is much less than that.
    F32MetadataIndexWriter(
        BlockfileWriter,
        // We use this to implement updates which require read-then-write semantics.
        Option<MetadataIndexReader<'me>>,
        #[allow(clippy::type_complexity)]
        Arc<tokio::sync::Mutex<HashMap<String, Vec<(f32, RoaringBitmap)>>>>,
    ),
    BoolMetadataIndexWriter(
        BlockfileWriter,
        // We use this to implement updates which require read-then-write semantics.
        Option<MetadataIndexReader<'me>>,
        Arc<tokio::sync::Mutex<HashMap<String, HashMap<bool, RoaringBitmap>>>>,
    ),
}

impl<'me> MetadataIndexWriter<'me> {
    pub fn new_string(
        init_blockfile_writer: BlockfileWriter,
        string_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    ) -> Self {
        MetadataIndexWriter::StringMetadataIndexWriter(
            init_blockfile_writer,
            string_metadata_index_reader,
            Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        )
    }

    pub fn new_u32(
        init_blockfile_writer: BlockfileWriter,
        int_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    ) -> Self {
        MetadataIndexWriter::U32MetadataIndexWriter(
            init_blockfile_writer,
            int_metadata_index_reader,
            Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        )
    }

    pub fn new_f32(
        init_blockfile_writer: BlockfileWriter,
        f32_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    ) -> Self {
        MetadataIndexWriter::F32MetadataIndexWriter(
            init_blockfile_writer,
            f32_metadata_index_reader,
            Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        )
    }

    pub fn new_bool(
        init_blockfile_writer: BlockfileWriter,
        bool_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    ) -> Self {
        MetadataIndexWriter::BoolMetadataIndexWriter(
            init_blockfile_writer,
            bool_metadata_index_reader,
            Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        )
    }

    // This is a helper function to make sure a key exists in the uncommitted_rbs
    // map. If `uncommitted` doesn't have an entry at (prefix, key), this function
    // will populate it from the blockfile reader. If the blockfile reader doesn't
    // have an entry, it will insert an empty RoaringBitmap.
    async fn look_up_key_and_populate_uncommitted_rbms(
        &self,
        prefix: &str,
        key: &KeyWrapper,
    ) -> Result<(), MetadataIndexError> {
        match self {
            MetadataIndexWriter::StringMetadataIndexWriter(_, reader, uncommitted_rbms) => {
                match key {
                    KeyWrapper::String(k) => {
                        let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                        if !uncommitted_rbms.contains_key(prefix) {
                            uncommitted_rbms.insert(prefix.to_string(), HashMap::new());
                        }
                        let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                        if !rbms.contains_key(k) {
                            let written_state = match reader {
                                Some(reader) => match reader.get(prefix, key).await {
                                    Ok(rbm) => rbm,
                                    // TODO: this should be more granular in its error handling
                                    Err(_) => RoaringBitmap::new(),
                                },
                                None => RoaringBitmap::new(),
                            };
                            rbms.insert(k.to_string(), written_state);
                        }
                    }
                    _ => return Err(MetadataIndexError::InvalidKeyType),
                }
            }
            MetadataIndexWriter::U32MetadataIndexWriter(_, reader, uncommitted_rbms) => match key {
                KeyWrapper::Uint32(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    if !uncommitted_rbms.contains_key(prefix) {
                        uncommitted_rbms.insert(prefix.to_string(), HashMap::new());
                    }
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    if !rbms.contains_key(k) {
                        let written_state = match reader {
                            Some(reader) => match reader.get(prefix, key).await {
                                Ok(rbm) => rbm,
                                Err(_) => RoaringBitmap::new(),
                            },
                            None => RoaringBitmap::new(),
                        };
                        rbms.insert(*k, written_state);
                    }
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexWriter::F32MetadataIndexWriter(_, reader, uncommitted_rbms) => match key {
                KeyWrapper::Float32(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    if !uncommitted_rbms.contains_key(prefix) {
                        uncommitted_rbms.insert(prefix.to_string(), Vec::new());
                    }
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    if !rbms.iter().any(|(rbm_k, _)| rbm_k == k) {
                        let written_state = match reader {
                            Some(reader) => match reader.get(prefix, key).await {
                                Ok(rbm) => rbm,
                                Err(_) => RoaringBitmap::new(),
                            },
                            None => RoaringBitmap::new(),
                        };
                        rbms.push((*k, written_state));
                    }
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexWriter::BoolMetadataIndexWriter(_, reader, uncommitted_rbms) => {
                match key {
                    KeyWrapper::Bool(k) => {
                        let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                        if !uncommitted_rbms.contains_key(prefix) {
                            uncommitted_rbms.insert(prefix.to_string(), HashMap::new());
                        }
                        let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                        if !rbms.contains_key(k) {
                            let written_state = match reader {
                                Some(reader) => match reader.get(prefix, key).await {
                                    Ok(rbm) => rbm,
                                    Err(_) => RoaringBitmap::new(),
                                },
                                None => RoaringBitmap::new(),
                            };
                            rbms.insert(*k, written_state);
                        }
                    }
                    _ => return Err(MetadataIndexError::InvalidKeyType),
                }
            }
        }
        Ok(())
    }

    pub async fn set<K: Into<KeyWrapper>>(
        &self,
        prefix: &str,
        key: K,
        offset_id: u32,
    ) -> Result<(), MetadataIndexError> {
        let key = key.into();
        self.look_up_key_and_populate_uncommitted_rbms(prefix, &key)
            .await?;
        match self {
            MetadataIndexWriter::StringMetadataIndexWriter(_, _, uncommitted_rbms) => match key {
                KeyWrapper::String(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.get_mut(&k).unwrap();
                    rbm.insert(offset_id);
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexWriter::BoolMetadataIndexWriter(_, _, uncommitted_rbms) => match key {
                KeyWrapper::Bool(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.get_mut(&k).unwrap();
                    rbm.insert(offset_id);
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexWriter::U32MetadataIndexWriter(_, _, uncommitted_rbms) => match key {
                KeyWrapper::Uint32(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.get_mut(&k).unwrap();
                    rbm.insert(offset_id);
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexWriter::F32MetadataIndexWriter(_, _, uncommitted_rbms) => match key {
                KeyWrapper::Float32(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.iter_mut().find(|(rbm_k, _)| *rbm_k == k).unwrap();
                    rbm.1.insert(offset_id);
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
        }
        Ok(())
    }

    pub async fn delete<K: Into<KeyWrapper>>(
        &self,
        prefix: &str,
        key: K,
        offset_id: u32,
    ) -> Result<(), MetadataIndexError> {
        let key = key.into();
        self.look_up_key_and_populate_uncommitted_rbms(prefix, &key)
            .await?;
        match self {
            MetadataIndexWriter::StringMetadataIndexWriter(_, _, uncommitted_rbms) => match key {
                KeyWrapper::String(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.get_mut(&k).unwrap();
                    rbm.remove(offset_id);
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexWriter::BoolMetadataIndexWriter(_, _, uncommitted_rbms) => match key {
                KeyWrapper::Bool(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.get_mut(&k).unwrap();
                    rbm.remove(offset_id);
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexWriter::U32MetadataIndexWriter(_, _, uncommitted_rbms) => match key {
                KeyWrapper::Uint32(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.get_mut(&k).unwrap();
                    rbm.remove(offset_id);
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexWriter::F32MetadataIndexWriter(_, _, uncommitted_rbms) => match key {
                KeyWrapper::Float32(k) => {
                    let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                    let rbms = uncommitted_rbms.get_mut(prefix).unwrap();
                    let rbm = rbms.iter_mut().find(|(rbm_k, _)| *rbm_k == k).unwrap();
                    rbm.1.remove(offset_id);
                }
                _ => return Err(MetadataIndexError::InvalidKeyType),
            },
        }
        Ok(())
    }

    pub async fn update(
        &self,
        prefix: &str,
        old_key: KeyWrapper,
        new_key: KeyWrapper,
        offset_id: u32,
    ) -> Result<(), MetadataIndexError> {
        self.delete(prefix, old_key, offset_id).await?;
        self.set(prefix, new_key, offset_id).await
    }

    pub async fn write_to_blockfile(&mut self) -> Result<(), MetadataIndexError> {
        match self {
            MetadataIndexWriter::StringMetadataIndexWriter(
                blockfile_writer,
                _,
                uncommitted_rbms,
            ) => {
                let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                for (prefix, mut rbms) in uncommitted_rbms.drain() {
                    for (key, rbm) in rbms.drain() {
                        match blockfile_writer
                            .set(prefix.as_str(), key.as_str(), rbm)
                            .await
                        {
                            Ok(_) => {}
                            Err(e) => return Err(MetadataIndexError::BlockfileError(e)),
                        }
                    }
                }
            }
            MetadataIndexWriter::U32MetadataIndexWriter(blockfile_writer, _, uncommitted_rbms) => {
                let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                for (prefix, mut rbms) in uncommitted_rbms.drain() {
                    for (key, rbm) in rbms.drain() {
                        match blockfile_writer.set(prefix.as_str(), key, rbm).await {
                            Ok(_) => {}
                            Err(e) => return Err(MetadataIndexError::BlockfileError(e)),
                        }
                    }
                }
            }
            MetadataIndexWriter::F32MetadataIndexWriter(blockfile_writer, _, uncommitted_rbms) => {
                let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                for (prefix, mut rbms) in uncommitted_rbms.drain() {
                    for (key, rbm) in rbms.drain(..) {
                        match blockfile_writer.set(prefix.as_str(), key, rbm).await {
                            Ok(_) => {}
                            Err(e) => return Err(MetadataIndexError::BlockfileError(e)),
                        }
                    }
                }
            }
            MetadataIndexWriter::BoolMetadataIndexWriter(blockfile_writer, _, uncommitted_rbms) => {
                let mut uncommitted_rbms = uncommitted_rbms.lock().await;
                for (prefix, mut rbms) in uncommitted_rbms.drain() {
                    for (key, rbm) in rbms.drain() {
                        match blockfile_writer.set(prefix.as_str(), key, rbm).await {
                            Ok(_) => {}
                            Err(e) => return Err(MetadataIndexError::BlockfileError(e)),
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn commit(self) -> Result<MetadataIndexFlusher, MetadataIndexError> {
        match self {
            MetadataIndexWriter::StringMetadataIndexWriter(blockfile_writer, _, _) => {
                match blockfile_writer.commit::<&str, RoaringBitmap>().await {
                    Ok(flusher) => Ok(MetadataIndexFlusher::StringMetadataIndexFlusher(flusher)),
                    Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                }
            }
            MetadataIndexWriter::U32MetadataIndexWriter(blockfile_writer, _, _) => {
                match blockfile_writer.commit::<u32, RoaringBitmap>().await {
                    Ok(flusher) => Ok(MetadataIndexFlusher::U32MetadataIndexFlusher(flusher)),
                    Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                }
            }
            MetadataIndexWriter::F32MetadataIndexWriter(blockfile_writer, _, _) => {
                match blockfile_writer.commit::<f32, RoaringBitmap>().await {
                    Ok(flusher) => Ok(MetadataIndexFlusher::F32MetadataIndexFlusher(flusher)),
                    Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                }
            }
            MetadataIndexWriter::BoolMetadataIndexWriter(blockfile_writer, _, _) => {
                match blockfile_writer.commit::<bool, RoaringBitmap>().await {
                    Ok(flusher) => Ok(MetadataIndexFlusher::BoolMetadataIndexFlusher(flusher)),
                    Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                }
            }
        }
    }
}

pub enum MetadataIndexFlusher {
    StringMetadataIndexFlusher(BlockfileFlusher),
    U32MetadataIndexFlusher(BlockfileFlusher),
    F32MetadataIndexFlusher(BlockfileFlusher),
    BoolMetadataIndexFlusher(BlockfileFlusher),
}

impl MetadataIndexFlusher {
    pub async fn flush(self) -> Result<(), MetadataIndexError> {
        match self {
            MetadataIndexFlusher::StringMetadataIndexFlusher(flusher) => {
                match flusher.flush::<&str, RoaringBitmap>().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                }
            }
            MetadataIndexFlusher::U32MetadataIndexFlusher(flusher) => {
                match flusher.flush::<u32, RoaringBitmap>().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                }
            }
            MetadataIndexFlusher::F32MetadataIndexFlusher(flusher) => {
                match flusher.flush::<f32, RoaringBitmap>().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                }
            }
            MetadataIndexFlusher::BoolMetadataIndexFlusher(flusher) => {
                match flusher.flush::<bool, RoaringBitmap>().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                }
            }
        }
    }

    pub fn id(&self) -> Uuid {
        match self {
            MetadataIndexFlusher::StringMetadataIndexFlusher(flusher) => flusher.id(),
            MetadataIndexFlusher::U32MetadataIndexFlusher(flusher) => flusher.id(),
            MetadataIndexFlusher::F32MetadataIndexFlusher(flusher) => flusher.id(),
            MetadataIndexFlusher::BoolMetadataIndexFlusher(flusher) => flusher.id(),
        }
    }
}

#[derive(Clone)]
pub enum MetadataIndexReader<'me> {
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
        metadata_key: &str,
        metadata_value: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, MetadataIndexError> {
        match self {
            MetadataIndexReader::StringMetadataIndexReader(blockfile_reader) => {
                match metadata_value {
                    KeyWrapper::String(k) => {
                        if !blockfile_reader.contains(metadata_key, k).await? {
                            return Ok(RoaringBitmap::new());
                        }
                        let rbm = blockfile_reader.get(metadata_key, k).await;
                        match rbm {
                            Ok(rbm) => Ok(rbm),
                            Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                        }
                    }
                    _ => Err(MetadataIndexError::InvalidKeyType),
                }
            }
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Uint32(k) => {
                    if !blockfile_reader.contains(metadata_key, *k).await? {
                        return Ok(RoaringBitmap::new());
                    }
                    let rbm = blockfile_reader.get(metadata_key, *k).await;
                    match rbm {
                        Ok(rbm) => Ok(rbm),
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Float32(k) => {
                    if !blockfile_reader.contains(metadata_key, *k).await? {
                        return Ok(RoaringBitmap::new());
                    }
                    let rbm = blockfile_reader.get(metadata_key, *k).await;
                    match rbm {
                        Ok(rbm) => Ok(rbm),
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexReader::BoolMetadataIndexReader(blockfile_reader) => {
                match metadata_value {
                    KeyWrapper::Bool(k) => {
                        if !blockfile_reader.contains(metadata_key, *k).await? {
                            return Ok(RoaringBitmap::new());
                        }
                        let rbm = blockfile_reader.get(metadata_key, *k).await;
                        match rbm {
                            Ok(rbm) => Ok(rbm),
                            Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                        }
                    }
                    _ => Err(MetadataIndexError::InvalidKeyType),
                }
            }
        }
    }

    pub async fn lt(
        &'me self,
        metadata_key: &str,
        metadata_value: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, MetadataIndexError> {
        match self {
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Uint32(k) => {
                    let read = blockfile_reader
                        .get_range(metadata_key..metadata_key, ..*k)
                        .await;
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Float32(k) => {
                    let read = blockfile_reader
                        .get_range(metadata_key..metadata_key, ..*k)
                        .await;
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            _ => Err(MetadataIndexError::InvalidKeyType),
        }
    }

    pub async fn lte(
        &'me self,
        metadata_key: &str,
        metadata_value: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, MetadataIndexError> {
        match self {
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Uint32(k) => {
                    let read = blockfile_reader
                        .get_range(metadata_key..=metadata_key, ..=*k)
                        .await;
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Float32(k) => {
                    let read = blockfile_reader
                        .get_range(metadata_key..=metadata_key, ..=*k)
                        .await;
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            _ => Err(MetadataIndexError::InvalidKeyType),
        }
    }

    pub async fn gt(
        &'me self,
        metadata_key: &str,
        metadata_value: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, MetadataIndexError> {
        match self {
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Uint32(k) => {
                    let read = blockfile_reader
                        .get_range(
                            metadata_key..=metadata_key,
                            (Bound::Excluded(*k), Bound::Unbounded),
                        )
                        .await;
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Float32(k) => {
                    let read = blockfile_reader
                        .get_range(
                            metadata_key..=metadata_key,
                            (Bound::Excluded(*k), Bound::Unbounded),
                        )
                        .await;
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            _ => Err(MetadataIndexError::InvalidKeyType),
        }
    }

    pub async fn gte(
        &'me self,
        metadata_key: &str,
        metadata_value: &'me KeyWrapper,
    ) -> Result<RoaringBitmap, MetadataIndexError> {
        match self {
            MetadataIndexReader::U32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Uint32(k) => {
                    let read = blockfile_reader
                        .get_range(metadata_key..=metadata_key, *k..)
                        .await;
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            MetadataIndexReader::F32MetadataIndexReader(blockfile_reader) => match metadata_value {
                KeyWrapper::Float32(k) => {
                    let read = blockfile_reader
                        .get_range(metadata_key..=metadata_key, *k..)
                        .await;
                    match read {
                        Ok(records) => {
                            let mut result = RoaringBitmap::new();
                            for (_, rbm) in records {
                                result = result.bitor(&rbm);
                            }
                            Ok(result)
                        }
                        Err(e) => Err(MetadataIndexError::BlockfileError(e)),
                    }
                }
                _ => Err(MetadataIndexError::InvalidKeyType),
            },
            _ => Err(MetadataIndexError::InvalidKeyType),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chroma_blockstore::provider::BlockfileProvider;

    #[tokio::test]
    async fn test_new_string_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, RoaringBitmap>().unwrap();
        let _writer = MetadataIndexWriter::new_string(blockfile_writer, None);
    }

    #[tokio::test]
    async fn test_new_u32_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<u32, RoaringBitmap>().unwrap();
        let _writer = MetadataIndexWriter::new_u32(blockfile_writer, None);
    }

    #[tokio::test]
    async fn test_new_f32_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<f32, RoaringBitmap>().unwrap();
        let _writer = MetadataIndexWriter::new_f32(blockfile_writer, None);
    }

    #[tokio::test]
    async fn test_new_bool_writer() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<bool, RoaringBitmap>().unwrap();
        let _writer = MetadataIndexWriter::new_bool(blockfile_writer, None);
    }

    #[tokio::test]
    async fn test_new_string_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let blockfile_writer = provider.create::<&str, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut md_writer = MetadataIndexWriter::new_string(blockfile_writer, None);
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
        let blockfile_writer = provider.create::<u32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut md_writer = MetadataIndexWriter::new_u32(blockfile_writer, None);
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
        let blockfile_writer = provider.create::<f32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut md_writer = MetadataIndexWriter::new_f32(blockfile_writer, None);
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
        let blockfile_writer = provider.create::<bool, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut md_writer = MetadataIndexWriter::new_bool(blockfile_writer, None);
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
        let blockfile_writer = provider.create::<&str, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_string(blockfile_writer, None);
        writer.set("key", "value", 1).await.unwrap();
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
        let blockfile_writer = provider.create::<u32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer, None);
        writer.set("key", 1, 1).await.unwrap();
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
        let blockfile_writer = provider.create::<f32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer, None);
        writer.set("key", 1.0, 1).await.unwrap();
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
        let blockfile_writer = provider.create::<bool, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_bool(blockfile_writer, None);
        writer.set("key", true, 1).await.unwrap();
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
        let blockfile_writer = provider.create::<&str, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_string(blockfile_writer, None);
        writer.set("key1", "value", 1).await.unwrap();
        writer.set("key1", "value", 2).await.unwrap();
        writer.set("key2", "value", 3).await.unwrap();
        writer.set("key2", "value2", 4).await.unwrap();
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
        let blockfile_writer = provider.create::<u32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer, None);
        writer.set("key1", 1, 1).await.unwrap();
        writer.set("key1", 1, 2).await.unwrap();
        writer.set("key2", 1, 3).await.unwrap();
        writer.set("key2", 2, 4).await.unwrap();
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
        let blockfile_writer = provider.create::<f32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer, None);
        writer.set("key1", 1.0, 1).await.unwrap();
        writer.set("key1", 1.0, 2).await.unwrap();
        writer.set("key2", 1.0, 3).await.unwrap();
        writer.set("key2", 2.0, 4).await.unwrap();
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
        let blockfile_writer = provider.create::<bool, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_bool(blockfile_writer, None);
        writer.set("key1", true, 1).await.unwrap();
        writer.set("key1", true, 2).await.unwrap();
        writer.set("key2", true, 3).await.unwrap();
        writer.set("key2", false, 4).await.unwrap();
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
        let blockfile_writer = provider.create::<u32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer, None);
        writer.set("key1", 1, 1).await.unwrap();
        writer.set("key1", 2, 2).await.unwrap();
        writer.set("key1", 3, 3).await.unwrap();
        writer.set("key1", 4, 4).await.unwrap();
        writer.set("key2", 5, 5).await.unwrap();
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
        let blockfile_writer = provider.create::<u32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer, None);
        writer.set("key1", 1, 1).await.unwrap();
        writer.set("key1", 2, 2).await.unwrap();
        writer.set("key1", 3, 3).await.unwrap();
        writer.set("key1", 4, 4).await.unwrap();
        writer.set("key2", 5, 5).await.unwrap();
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
        let blockfile_writer = provider.create::<u32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer, None);
        writer.set("key1", 1, 1).await.unwrap();
        writer.set("key1", 2, 2).await.unwrap();
        writer.set("key1", 3, 3).await.unwrap();
        writer.set("key1", 4, 4).await.unwrap();
        writer.set("key2", 5, 5).await.unwrap();
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
        let blockfile_writer = provider.create::<u32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_u32(blockfile_writer, None);
        writer.set("key1", 1, 1).await.unwrap();
        writer.set("key1", 2, 2).await.unwrap();
        writer.set("key1", 3, 3).await.unwrap();
        writer.set("key1", 4, 4).await.unwrap();
        writer.set("key2", 5, 5).await.unwrap();
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
        let blockfile_writer = provider.create::<f32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer, None);
        writer.set("key1", 1.0, 1).await.unwrap();
        writer.set("key1", 2.0, 2).await.unwrap();
        writer.set("key1", 3.0, 3).await.unwrap();
        writer.set("key1", 4.0, 4).await.unwrap();
        writer.set("key2", 5.0, 5).await.unwrap();
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
        let blockfile_writer = provider.create::<f32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer, None);
        writer.set("key1", 1.0, 1).await.unwrap();
        writer.set("key1", 2.0, 2).await.unwrap();
        writer.set("key1", 3.0, 3).await.unwrap();
        writer.set("key1", 4.0, 4).await.unwrap();
        writer.set("key2", 5.0, 5).await.unwrap();
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
        let blockfile_writer = provider.create::<f32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer, None);
        writer.set("key1", 1.0, 1).await.unwrap();
        writer.set("key1", 2.0, 2).await.unwrap();
        writer.set("key1", 3.0, 3).await.unwrap();
        writer.set("key1", 4.0, 4).await.unwrap();
        writer.set("key2", 5.0, 5).await.unwrap();
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
        let blockfile_writer = provider.create::<f32, RoaringBitmap>().unwrap();
        let writer_id = blockfile_writer.id();
        let mut writer = MetadataIndexWriter::new_f32(blockfile_writer, None);
        writer.set("key1", 1.0, 1).await.unwrap();
        writer.set("key1", 2.0, 2).await.unwrap();
        writer.set("key1", 3.0, 3).await.unwrap();
        writer.set("key1", 4.0, 4).await.unwrap();
        writer.set("key2", 5.0, 5).await.unwrap();
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

    // TODO enable this test once fork() is enabled for MemoryBlockfiles.
    // #[tokio::test]
    // async fn test_set_get_set_delete() {
    //     let provider = BlockfileProvider::new_memory();
    //     let blockfile_writer = provider.create::<u32, &RoaringBitmap>().unwrap();
    //     let writer_id = blockfile_writer.id();
    //     let mut writer = MetadataIndexWriter::new_u32(blockfile_writer, None);
    //     writer.set("key1", 1, 1).await.unwrap();
    //     writer.write_to_blockfile().await.unwrap();
    //     let flusher = writer.commit().await.unwrap();
    //     flusher.flush().await.unwrap();

    //     let blockfile_reader = provider
    //         .open::<u32, RoaringBitmap>(&writer_id)
    //         .await
    //         .unwrap();
    //     let reader = MetadataIndexReader::new_u32(blockfile_reader);
    //     let bitmap = reader.get("key1", &1.into()).await.unwrap();
    //     assert_eq!(bitmap.len(), 1);
    //     assert!(bitmap.contains(1));

    //     let blockfile_writer = provider
    //         .fork::<u32, &RoaringBitmap>(&writer_id)
    //         .await
    //         .unwrap();
    //     let mut writer = MetadataIndexWriter::new_u32(blockfile_writer, Some(reader));
    //     writer.set("key1", 1, 2).await.unwrap();
    //     writer.write_to_blockfile().await.unwrap();
    //     let flusher = writer.commit().await.unwrap();
    //     flusher.flush().await.unwrap();

    //     let blockfile_reader = provider
    //         .open::<u32, RoaringBitmap>(&writer_id)
    //         .await
    //         .unwrap();
    //     let reader = MetadataIndexReader::new_u32(blockfile_reader);
    //     let bitmap = reader.get("key1", &1.into()).await.unwrap();
    //     assert_eq!(bitmap.len(), 2);
    //     assert!(bitmap.contains(1));
    //     assert!(bitmap.contains(2));
    // }
}
