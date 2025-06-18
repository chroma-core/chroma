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
    let fragments = reader.scan(offset, Limits::UNLIMITED).await?;
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
        Manifest::initialize(options, storage, &target, "empty copy task").await?;
    }
    Ok(())
}
