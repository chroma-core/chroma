use chroma_storage::Storage;
use setsum::Setsum;

use crate::manifest::{unprefixed_snapshot_path, Manifest, Snapshot};
use crate::reader::{read_parquet, LogReader};
use crate::writer::upload_parquet;
use crate::{Error, Fragment, LogPosition, LogWriterOptions, SnapshotPointer};

pub async fn copy_snapshot(
    storage: &Storage,
    options: &LogWriterOptions,
    root: &SnapshotPointer,
    offset: LogPosition,
    target: &str,
) -> Result<SnapshotPointer, Error> {
    let Some(snapshot) = Snapshot::load(&options.throttle_manifest, storage, target, root).await?
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
            snapshots
                .push(Box::pin(copy_snapshot(storage, options, snapshot, offset, target)).await?);
        } else {
            dropped.push(snapshot.setsum);
        }
    }
    let mut fragments = vec![];
    for fragment in &snapshot.fragments {
        if fragment.limit > offset {
            fragments.push(copy_fragment(storage, options, fragment, target).await?);
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
    let depth = snapshots.iter().map(|x| x.depth).max().unwrap_or(0);
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
    frag: &Fragment,
    target: &str,
) -> Result<Fragment, Error> {
    let (setsum1, data, _) = read_parquet(storage, target, &frag.path).await?;
    if data.is_empty() {
        return Err(Error::CorruptFragment(format!("{} has no data", frag.path)));
    }
    let start = data[0].0;
    let limit = start + data.len();
    let messages = data.into_iter().map(|x| x.1).collect();
    let (path, setsum2, num_bytes) =
        upload_parquet(options, storage, target, frag.seq_no, start, messages).await?;
    let num_bytes = num_bytes as u64;
    if setsum1 != setsum2 {
        return Err(Error::CorruptFragment(format!(
            "{} download setsum ({}) does not match upload setsum ({})",
            frag.path,
            setsum1.hexdigest(),
            setsum2.hexdigest()
        )));
    }
    let setsum = setsum1;
    Ok(Fragment {
        path,
        seq_no: frag.seq_no,
        start,
        limit,
        num_bytes,
        setsum,
    })
}

pub async fn copy(
    storage: &Storage,
    options: &LogWriterOptions,
    reader: LogReader,
    offset: LogPosition,
    target: String,
) -> Result<(), Error> {
    let Some(manifest) = reader.manifest().await? else {
        return Err(Error::UninitializedLog);
    };
    let mut snapshots = vec![];
    for snapshot in &manifest.snapshots {
        snapshots.push(copy_snapshot(storage, options, snapshot, offset, &target).await?);
    }
    let mut fragments = vec![];
    for fragment in &manifest.fragments {
        fragments.push(copy_fragment(storage, options, fragment, &target).await?);
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
