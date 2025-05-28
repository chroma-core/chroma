use chroma_storage::Storage;
use setsum::Setsum;

use crate::manifest::{unprefixed_snapshot_path, Manifest, Snapshot};
use crate::reader::LogReader;
use crate::writer::copy_parquet;
use crate::{Error, Fragment, LogPosition, LogWriterOptions, SnapshotPointer};

pub async fn copy_snapshot(
    storage: &Storage,
    options: &LogWriterOptions,
    reader: &LogReader,
    root: &SnapshotPointer,
    offset: LogPosition,
    target: &str,
) -> Result<SnapshotPointer, Error> {
    let Some(snapshot) =
        Snapshot::load(&options.throttle_manifest, storage, &reader.prefix, root).await?
    else {
        return Err(Error::CorruptManifest(format!(
            "snapshot {} does not exist",
            root.setsum.hexdigest(),
        )));
    };
    let mut dropped = vec![];
    let mut snapshots = vec![];
    for snapshot in &snapshot.snapshots {
        if snapshot.limit > offset {
            snapshots.push(
                Box::pin(copy_snapshot(
                    storage, options, reader, snapshot, offset, target,
                ))
                .await?,
            );
        } else {
            dropped.push(snapshot.setsum);
        }
    }
    let mut fragments = vec![];
    for fragment in &snapshot.fragments {
        if fragment.limit > offset {
            fragments.push(copy_fragment(storage, options, reader, fragment, target).await?);
        } else {
            dropped.push(fragment.setsum);
        }
    }
    let dropped = dropped.iter().fold(Setsum::default(), |x, y| x + *y);
    let kept_snapshots = snapshots
        .iter()
        .fold(Setsum::default(), |x, y| x + y.setsum);
    let kept_fragments = fragments
        .iter()
        .fold(Setsum::default(), |x, y| x + y.setsum);
    if dropped + kept_snapshots + kept_fragments != root.setsum {
        // NOTE(rescrv):  If you see this error you have to figure out where data is lost.  This
        // will require writing a test case rather than trying to deduce it from the setsums.
        return Err(Error::CorruptManifest(
            "Copying failed because the setsum was not balanced".to_string(),
        ));
    }
    let depth = snapshots.iter().map(|x| x.depth + 1).max().unwrap_or(0);
    let snapshot = Snapshot {
        path: unprefixed_snapshot_path(kept_snapshots + kept_fragments),
        depth,
        setsum: kept_snapshots + kept_fragments,
        writer: "copy task".to_string(),
        snapshots,
        fragments,
    };
    snapshot
        .install(&options.throttle_manifest, storage, target)
        .await
}

pub async fn copy_fragment(
    storage: &Storage,
    options: &LogWriterOptions,
    reader: &LogReader,
    frag: &Fragment,
    target: &str,
) -> Result<Fragment, Error> {
    copy_parquet(
        options,
        storage,
        &format!("{}/{}", reader.prefix, frag.path),
        &format!("{}/{}", target, frag.path),
    )
    .await?;
    Ok(frag.clone())
}

pub async fn copy(
    storage: &Storage,
    options: &LogWriterOptions,
    reader: &LogReader,
    offset: LogPosition,
    target: String,
) -> Result<(), Error> {
    let manifest = reader
        .manifest()
        .await?
        .unwrap_or(Manifest::new_empty("copy task"));
    let mut snapshots = vec![];
    for snapshot in &manifest.snapshots {
        snapshots.push(copy_snapshot(storage, options, reader, snapshot, offset, &target).await?);
    }
    let mut fragments = vec![];
    for fragment in &manifest.fragments {
        fragments.push(copy_fragment(storage, options, reader, fragment, &target).await?);
    }
    let setsum = snapshots
        .iter()
        .map(|x| x.setsum)
        .fold(Setsum::default(), |x, y| x + y)
        + fragments
            .iter()
            .map(|x| x.setsum)
            .fold(Setsum::default(), |x, y| x + y);
    let acc_bytes = snapshots.iter().map(|x| x.num_bytes).sum::<u64>()
        + fragments.iter().map(|x| x.num_bytes).sum::<u64>();
    let manifest = Manifest {
        setsum,
        acc_bytes,
        writer: "copy task".to_string(),
        snapshots,
        fragments,
    };
    Manifest::initialize_from_manifest(options, storage, &target, manifest).await?;
    Ok(())
}
