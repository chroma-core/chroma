use super::{HnswIndex, HnswIndexConfig, Index, IndexConfig};
use crate::index::types::PersistentIndex;
use crate::{errors::ChromaError, storage::Storage, types::Segment};
use parking_lot::RwLock;
use std::fmt::Debug;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::io::AsyncBufReadExt;
use uuid::Uuid;

// These are the files hnswlib writes to disk. This is strong coupling, but we need to know
// what files to read from disk. We could in the future have the C++ code return the files
// but ideally we have a rust implementation of hnswlib
const FILES: [&'static str; 4] = [
    "header.bin",
    "data_level0.bin",
    "length.bin",
    "link_lists.bin",
];

#[derive(Clone)]
pub(crate) struct HnswIndexProvider {
    cache: Arc<RwLock<HashMap<Uuid, Arc<RwLock<HnswIndex>>>>>,
    pub(crate) temporary_storage_path: PathBuf,
    storage: Box<Storage>,
}

impl Debug for HnswIndexProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HnswIndexProvider {{ temporary_storage_path: {:?}, cache: {} }}",
            self.temporary_storage_path,
            self.cache.read().len(),
        )
    }
}

impl HnswIndexProvider {
    pub(crate) fn new(storage: Box<Storage>, storage_path: PathBuf) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            storage,
            temporary_storage_path: storage_path,
        }
    }

    pub(crate) fn get(&self, id: &Uuid) -> Option<Arc<RwLock<HnswIndex>>> {
        let cache = self.cache.read();
        cache.get(id).cloned()
    }

    // TODO: THIS SHOULD BE FORK AND SHOULD NOT OVERWITE THE SAME HNSW INDEX EVERYTIME
    pub(crate) async fn load(
        &self,
        id: &Uuid,
        segment: &Segment,
        dimensionality: i32,
    ) -> Result<Arc<RwLock<HnswIndex>>, Box<dyn ChromaError>> {
        let index_storage_path = self.temporary_storage_path.join(id.to_string());
        self.create_dir_all(&index_storage_path)?;

        // Fetch the files from storage and put them in the index storage path
        for file in FILES.iter() {
            // TOOD: put key formatting as function
            let key = format!("hnsw/{}/{}", id, file);
            println!("Loading hnsw index file: {}", key);
            let res = self.storage.get(&key).await;
            let mut reader = match res {
                Ok(reader) => reader,
                Err(e) => {
                    // TODO: return Err(e);
                    panic!("Failed to load hnsw index file from storage: {}", e);
                }
            };

            let file_path = index_storage_path.join(file);
            // For now, we never evict from the cache, so if the index is being loaded, the file does not exist
            let file_handle = tokio::fs::File::create(&file_path).await;
            let mut file_handle = match file_handle {
                Ok(file) => file,
                Err(e) => {
                    // TODO: cleanup created files if this fails
                    panic!("Failed to create file: {}", e);
                }
            };
            let copy_res = tokio::io::copy(&mut reader, &mut file_handle).await;
            match copy_res {
                Ok(_) => {
                    println!(
                        "Copied storage key: {} to file: {}",
                        key,
                        file_path.to_str().unwrap()
                    );
                }
                Err(e) => {
                    // TODO: cleanup created files if this fails and error handle
                    panic!("Failed to copy file: {}", e);
                }
            }
            // bytes is an AsyncBufRead, so we fil and consume it to a file
            println!("Loaded hnsw index file: {}", file);
        }

        let index_config = IndexConfig::from_segment(&segment, dimensionality)?;
        let hnsw_config = HnswIndexConfig::from_segment(segment, &index_storage_path)?;
        // TODO: don't unwrap path conv here
        match HnswIndex::load(index_storage_path.to_str().unwrap(), &index_config, *id) {
            Ok(index) => {
                let index = Arc::new(RwLock::new(index));
                let mut cache = self.cache.write();
                cache.insert(*id, index.clone());
                Ok(index)
            }
            Err(e) => Err(e),
        }
    }

    // Compactor
    // Cases
    // A write comes in and no files are in the segment -> we know we need to create a new index
    // A write comes in and files are in the segment -> we know we need to load the index
    // If the writer drops, but we already have the index, the id will be in the cache and the next job will have files and not need to load the index

    // Query
    // Cases
    // A query comes in and the index is in the cache -> we can query the index based on segment files id (Same as compactor case 3 where we have the index)
    // A query comes in and the index is not in the cache -> we need to load the index from s3 based on the segment files id

    pub(crate) fn create(
        &self,
        // TODO: This should not take Segment. The index layer should not know about the segment concept
        segment: &Segment,
        dimensionality: i32,
    ) -> Result<Arc<RwLock<HnswIndex>>, Box<dyn ChromaError>> {
        let id = Uuid::new_v4();
        let index_storage_path = self.temporary_storage_path.join(id.to_string());
        self.create_dir_all(&index_storage_path)?;
        let index_config = IndexConfig::from_segment(&segment, dimensionality)?;
        let hnsw_config = HnswIndexConfig::from_segment(segment, &index_storage_path)?;
        let mut cache = self.cache.write();
        let index = Arc::new(RwLock::new(HnswIndex::init(
            &index_config,
            Some(&hnsw_config),
            id,
        )?));
        cache.insert(id, index.clone());
        Ok(index)
    }

    pub(crate) fn commit(&self, id: &Uuid) -> Result<(), Box<dyn ChromaError>> {
        let cache = self.cache.read();
        let index = match cache.get(id) {
            Some(index) => index,
            None => {
                // TODO: error
                panic!("Trying to commit index that doesn't exist");
            }
        };
        index.write().save()?;
        Ok(())
    }

    pub(crate) async fn flush(&self, id: &Uuid) -> Result<(), Box<dyn ChromaError>> {
        // Scope to drop the cache lock before we await to write to s3
        // TODO: since we commit(), we don't need to save the index here
        {
            let cache = self.cache.read();
            let index = match cache.get(id) {
                Some(index) => index,
                None => {
                    // TODO: error
                    panic!("Trying to flush index that doesn't exist");
                }
            };
            index.write().save()?;
        }
        let index_storage_path = self.temporary_storage_path.join(id.to_string());
        for file in FILES.iter() {
            let file_path = index_storage_path.join(file);
            let key = format!("hnsw/{}/{}", id, file);
            let res = self
                .storage
                .put_file(&key, file_path.to_str().unwrap())
                .await;
            match res {
                Ok(_) => {
                    println!("Flushed hnsw index file: {}", file);
                }
                Err(e) => {
                    println!("Failed to flush index: {}", e);
                    return Err(Box::new(e));
                }
            }
        }
        Ok(())
    }

    fn create_dir_all(&self, path: &PathBuf) -> Result<(), Box<dyn ChromaError>> {
        match std::fs::create_dir_all(path) {
            Ok(_) => Ok(()),
            Err(e) => {
                // TODO: return error
                panic!("Failed to create directory: {}", e);
            }
        }
    }
}
