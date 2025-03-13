// NOTE(rescrv):  All caches align to storage.  For now, implement without caching.  Caching
// should/could literally be a layer over storage, so add it later once correctness without caching
// is ensured by adequate testing.

use std::sync::Arc;

use chroma_storage::Storage;

use crate::{Error, Fragment, LogPosition, LogReaderOptions, Manifest, Snapshot};

/// Limits allows encoding things like offset, timestamp, and byte size limits for the read.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Limits {
    pub max_files: Option<u64>,
    pub max_bytes: Option<u64>,
}

/// LogReader is a reader for the log.
pub struct LogReader {
    options: LogReaderOptions,
    storage: Arc<Storage>,
    prefix: String,
}

impl LogReader {
    pub async fn open(
        options: LogReaderOptions,
        storage: Arc<Storage>,
        prefix: String,
    ) -> Result<Self, Error> {
        Ok(Self {
            options,
            storage,
            prefix,
        })
    }

    /// Scan up to, but not including the provided limit across three dimensions:
    /// 1. The offset of the log position.
    /// 2. The number of files to return.
    /// 3. The total number of bytes to return.
    pub async fn scan(&self, from: LogPosition, limits: Limits) -> Result<Vec<Fragment>, Error> {
        let Some((manifest, _)) = Manifest::load(&self.storage, &self.prefix).await? else {
            return Err(Error::UninitializedLog);
        };
        let mut snapshots = manifest
            .snapshots
            .iter()
            .filter(|s| s.limit.offset() > from.offset())
            .cloned()
            .collect::<Vec<_>>();
        let mut fragments = manifest
            .fragments
            .iter()
            .filter(|f| f.limit.offset() > from.offset())
            .cloned()
            .collect::<Vec<_>>();
        while !snapshots.is_empty() {
            // In parallel resolve this level of the tree.
            let futures = snapshots
                .iter()
                .map(|s| {
                    let options = self.options.clone();
                    let storage = Arc::clone(&self.storage);
                    async move { Snapshot::load(&options.throttle, &storage, s).await }
                })
                .collect::<Vec<_>>();
            let resolved = futures::future::try_join_all(futures).await?;
            // NOTE(rescrv):  This empties snapshots before the first loop so we can fill it
            // incrementally as we find snapshots that reference snapshots.
            for (r, s) in
                std::iter::zip(resolved.iter(), std::mem::take(&mut snapshots).into_iter())
            {
                if let Some(r) = r {
                    snapshots.extend(r.snapshots.iter().cloned());
                    fragments.extend(r.fragments.iter().cloned());
                } else {
                    return Err(Error::CorruptManifest(format!(
                        "snapshot {} is missing",
                        s.path_to_snapshot
                    )));
                }
            }
        }
        fragments.sort_by_key(|f| f.start.offset());
        fragments.truncate(limits.max_files.unwrap_or(u64::MAX) as usize);
        while fragments.len() > 1
            && fragments
                .iter()
                .map(|f| f.num_bytes)
                .fold(0, u64::saturating_add)
                > limits.max_bytes.unwrap_or(u64::MAX)
        {
            fragments.pop();
        }
        Ok(fragments)
    }

    pub async fn fetch(&self, fragment: &Fragment) -> Result<Arc<Vec<u8>>, Error> {
        let path = format!("{}/{}", self.prefix, fragment.path);
        Ok(self
            .storage
            .get_with_e_tag(&path)
            .await
            .map_err(Arc::new)?
            .0)
    }
}
