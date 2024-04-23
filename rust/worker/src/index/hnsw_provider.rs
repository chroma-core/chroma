use super::{HnswIndex, HnswIndexConfig, Index, IndexConfig};
use crate::index::types::PersistentIndex;
use crate::{errors::ChromaError, storage::Storage, types::Segment};
use parking_lot::RwLock;
use std::fmt::Debug;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
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
        segment: &Segment,
        dimensionality: i32,
    ) -> Result<Arc<RwLock<HnswIndex>>, Box<dyn ChromaError>> {
        let id = Uuid::new_v4();
        let index_storage_path = self.temporary_storage_path.join(id.to_string());
        // Create the storage path, if it doesn't exist
        match std::fs::create_dir_all(&index_storage_path) {
            Ok(_) => {}
            Err(e) => {
                // TODO: log error
                panic!("Failed to create index storage path: {}", e);
            }
        }
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
                    // TODO: return err
                    panic!("Failed to flush index: {}", e);
                }
            }
        }
        Ok(())
    }
}
