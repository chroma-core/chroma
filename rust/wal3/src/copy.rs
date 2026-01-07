use chroma_types::Cmek;
use setsum::Setsum;

use crate::interfaces::{FragmentPointer, FragmentPublisher, ManifestManagerFactory};
use crate::{Error, Limits, LogPosition, LogReaderTrait, Manifest};

/// Copy a log from one prefix to another.
///
/// The `manifest_factory` is used to initialize the target manifest. The target prefix is taken
/// from the factory.
pub async fn copy<
    P: FragmentPointer,
    FP: FragmentPublisher<FragmentPointer = P>,
    MF: ManifestManagerFactory<FragmentPointer = P>,
>(
    reader: &dyn LogReaderTrait,
    offset: LogPosition,
    fragment_publisher: &FP,
    manifest_factory: MF,
    cmek: Option<Cmek>,
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
        return Err(Error::internal(file!(), line!()));
    }
    if !fragments.is_empty() {
        let mut futures = vec![];
        for fragment in fragments.into_iter() {
            let pointer: P = P::try_create(fragment.seq_no, fragment.start)
                .ok_or_else(|| Error::internal(file!(), line!()))?;
            let cmek = cmek.clone();
            futures.push(async move {
                let (_, messages, _, ts) = match reader.read_parquet(&fragment).await {
                    Ok(x) => x,
                    Err(err) => return Err(err),
                };
                let messages = messages.into_iter().map(|(_, d)| d).collect::<Vec<_>>();
                fragment_publisher
                    .upload_parquet(&pointer, messages, cmek, ts)
                    .await?;
                Ok(fragment)
            });
        }
        let fragments = futures::future::try_join_all(futures).await?;
        let setsum = fragments
            .iter()
            .map(|x| x.setsum)
            .fold(Setsum::default(), |x, y| x + y);
        let collected = Setsum::default();
        let acc_bytes = fragments.iter().map(|x| x.num_bytes).sum::<u64>();
        let initial_offset = fragments.iter().map(|f| f.start).min();
        let initial_seq_no = fragments.iter().map(|f| f.seq_no).min();
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
        manifest_factory.init_manifest(&manifest).await?;
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
            return Err(Error::internal(file!(), line!()));
        }
        manifest_factory.init_manifest(&manifest).await?;
    }
    Ok(())
}
