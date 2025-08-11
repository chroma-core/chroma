use std::sync::Arc;

use chroma_storage::{DeleteOptions, GetOptions, Storage, StorageError};

use crate::cursors::CursorStore;
use crate::manifest::{snapshot_prefix, snapshot_setsum, unprefixed_manifest_path};
use crate::{
    fragment_prefix, parse_fragment_path, CursorStoreOptions, Error, Fragment, Manifest, Snapshot,
    SnapshotPointer, ThrottleOptions,
};

async fn destroy_snapshot(
    storage: &Arc<Storage>,
    prefix: &str,
    snap: &SnapshotPointer,
) -> Result<(), Error> {
    if let Some(snapshot) =
        Snapshot::load(&ThrottleOptions::default(), storage, prefix, snap).await?
    {
        for snap in snapshot.snapshots.iter() {
            Box::pin(destroy_snapshot(storage, prefix, snap)).await?;
        }
        for frag in snapshot.fragments.iter() {
            destroy_fragment(storage, prefix, frag).await?;
        }
    }
    delete_file(storage, prefix, &snap.path_to_snapshot).await
}

async fn destroy_fragment(
    storage: &Arc<Storage>,
    prefix: &str,
    frag: &Fragment,
) -> Result<(), Error> {
    delete_file(storage, prefix, &frag.path).await
}

async fn destroy_cursors(storage: &Arc<Storage>, prefix: &str) -> Result<(), Error> {
    let cstore = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::clone(storage),
        prefix.to_string(),
        "destroy".to_string(),
    );
    for cursor in cstore.list().await? {
        delete_file(storage, prefix, &cursor.path()).await?;
    }
    Ok(())
}

async fn destroy_garbage(storage: &Arc<Storage>, prefix: &str) -> Result<(), Error> {
    delete_file(storage, prefix, "gc/GARBAGE").await
}

async fn destroy_dangling_snapshots(storage: &Arc<Storage>, prefix: &str) -> Result<(), Error> {
    loop {
        let possible_snapshots = match storage
            .list_prefix(
                &format!("{}/{}", prefix, snapshot_prefix()),
                GetOptions::default(),
            )
            .await
            .map_err(Arc::new)
        {
            Ok(possible_fragments) => possible_fragments,
            Err(err) => {
                return Err(Error::StorageError(err));
            }
        };
        if possible_snapshots.is_empty() {
            return Ok(());
        }
        for snap_path in possible_snapshots {
            let Some(unprefixed_path) = snap_path.strip_prefix(prefix) else {
                return Err(Error::GarbageCollection(format!(
                    "got a snapshot I don't trust: {snap_path}"
                )));
            };
            let possible_snapshot = unprefixed_path.trim_start_matches('/');
            if let Ok(_setsum) = snapshot_setsum(possible_snapshot) {
                delete_file(storage, prefix, possible_snapshot).await?;
            } else {
                return Err(Error::GarbageCollection(format!(
                    "got a snapshot I don't trust: {snap_path}"
                )));
            }
        }
    }
}

async fn destroy_dangling_fragments(storage: &Arc<Storage>, prefix: &str) -> Result<(), Error> {
    loop {
        let possible_fragments = match storage
            .list_prefix(
                &format!("{}/{}", prefix, fragment_prefix()),
                GetOptions::default(),
            )
            .await
            .map_err(Arc::new)
        {
            Ok(possible_fragments) => possible_fragments,
            Err(err) => return Err(Error::StorageError(err)),
        };
        if possible_fragments.is_empty() {
            return Ok(());
        }
        for frag_path in possible_fragments {
            let Some(unprefixed_path) = frag_path.strip_prefix(prefix) else {
                return Err(Error::GarbageCollection(format!(
                    "got a fragment I don't trust: {frag_path}"
                )));
            };
            let possible_fragment = unprefixed_path.trim_start_matches('/');
            if parse_fragment_path(possible_fragment).is_some() {
                delete_file(storage, prefix, possible_fragment).await?;
            } else {
                return Err(Error::GarbageCollection(format!(
                    "got a fragment I don't trust: {frag_path}"
                )));
            }
        }
    }
}

async fn destroy_manifest(storage: &Arc<Storage>, prefix: &str) -> Result<(), Error> {
    delete_file(storage, prefix, &unprefixed_manifest_path()).await
}

async fn delete_file(
    storage: &Arc<Storage>,
    prefix: &str,
    relative_path: &str,
) -> Result<(), Error> {
    let path = format!("{}/{}", prefix, relative_path);
    match storage.delete(&path, DeleteOptions::default()).await {
        Ok(()) => Ok(()),
        Err(err) => match err {
            StorageError::NotFound { path: _, source: _ } => Ok(()),
            _ => Err(Arc::new(err).into()),
        },
    }
}

/// Destroys a wal3 log under the assumption that there are no concurrent writers.
pub async fn destroy(storage: Arc<Storage>, prefix: &str) -> Result<(), Error> {
    let Some((manifest, _)) = Manifest::load(&ThrottleOptions::default(), &storage, prefix).await?
    else {
        tracing::warn!("strategically refusing to erase {prefix} without a manifest");
        return Ok(());
    };
    for snapshot in manifest.snapshots.iter() {
        destroy_snapshot(&storage, prefix, snapshot).await?;
    }
    for fragment in manifest.fragments.iter() {
        destroy_fragment(&storage, prefix, fragment).await?;
    }
    destroy_cursors(&storage, prefix).await?;
    destroy_garbage(&storage, prefix).await?;
    destroy_dangling_snapshots(&storage, prefix).await?;
    destroy_dangling_fragments(&storage, prefix).await?;
    destroy_manifest(&storage, prefix).await?;
    let possible_files = storage
        .list_prefix(prefix, GetOptions::default())
        .await
        .map_err(Arc::new)?;
    if possible_files.is_empty() {
        Ok(())
    } else {
        tracing::error!("leftover files in {prefix}");
        Err(Error::GarbageCollection(format!(
            "got a file I don't trust: {}",
            possible_files[0],
        )))
    }
}
