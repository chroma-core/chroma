use std::sync::Arc;

use chroma_storage::Storage;
use setsum::Setsum;

use crate::reader::LogReader;
use crate::{
    prefixed_fragment_path, Error, FragmentSeqNo, Limits, LogPosition, LogWriterOptions, Manifest,
};

pub async fn copy(
    storage: &Storage,
    options: &LogWriterOptions,
    reader: &LogReader,
    offset: LogPosition,
    target: String,
) -> Result<(), Error> {
    let reference = reader
        .manifest()
        .await?
        .unwrap_or(Manifest::new_empty("zero-copy task"));
    let mut short_read = false;
    let fragments = reader
        .scan_with_cache(&reference, offset, Limits::UNLIMITED, &mut short_read)
        .await?;
    if short_read {
        tracing::error!("short_read in unlimited copy");
        return Err(Error::Internal);
    }
    if !fragments.is_empty() {
        let mut futures = vec![];
        for fragment in fragments.into_iter() {
            let target = &target;
            futures.push(async move {
                storage
                    .copy(
                        &prefixed_fragment_path(&reader.prefix, fragment.seq_no),
                        &prefixed_fragment_path(target, fragment.seq_no),
                    )
                    .await
                    .map(|_| fragment)
            });
        }
        let fragments = futures::future::try_join_all(futures)
            .await
            .map_err(Arc::new)?;
        let setsum = fragments
            .iter()
            .map(|x| x.setsum)
            .fold(Setsum::default(), |x, y| x + y);
        let collected = Setsum::default();
        let acc_bytes = fragments.iter().map(|x| x.num_bytes).sum::<u64>();
        let initial_offset = Some(fragments.iter().map(|f| f.start).min().unwrap_or(offset));
        let initial_seq_no = Some(
            fragments
                .iter()
                .map(|f| f.seq_no)
                .min()
                .unwrap_or(FragmentSeqNo::BEGIN),
        );
        let manifest = Manifest {
            setsum,
            collected,
            acc_bytes,
            writer: "copy task".to_string(),
            snapshots: vec![],
            fragments,
            initial_offset,
            initial_seq_no,
        };
        Manifest::initialize_from_manifest(options, storage, &target, manifest).await?;
    } else {
        let setsum = Setsum::default();
        let collected = Setsum::default();
        let acc_bytes = 0;
        let manifest = Manifest {
            setsum,
            collected,
            acc_bytes,
            writer: "zero-copy task".to_string(),
            snapshots: vec![],
            fragments: vec![],
            initial_offset: Some(reference.next_write_timestamp()),
            initial_seq_no: reference.next_fragment_seq_no(),
        };
        if manifest.initial_offset.is_some() && manifest.initial_seq_no.is_none() {
            return Err(Error::Internal);
        }
        Manifest::initialize_from_manifest(options, storage, &target, manifest).await?;
    }
    Ok(())
}
