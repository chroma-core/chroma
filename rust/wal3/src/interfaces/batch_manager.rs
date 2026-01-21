use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use setsum::Setsum;
use tracing::Span;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, ETag, PutMode, PutOptions, Storage, StorageError,
};
use chroma_types::Cmek;

use crate::backoff::ExponentialBackoff;
use crate::interfaces::{FragmentPointer, FragmentPublisher, ManifestPublisher, UploadResult};
use crate::{
    CursorStore, CursorStoreOptions, Error, FragmentIdentifier, Garbage, LogPosition,
    LogWriterOptions, ThrottleOptions,
};

use super::FragmentUploader;

/////////////////////////////////////////// ManagerState ///////////////////////////////////////////

/// ManagerState captures the state necessary to batch manifests.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
struct ManagerState {
    backoff: bool,
    next_write: Instant,
    writers_active: usize,
    enqueued: Vec<(
        Vec<Vec<u8>>,
        tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        Span,
    )>,
    tearing_down: bool,
}

impl ManagerState {
    /// Set the next_write instant based upon the current time and throttle options.  We wait at
    /// least the 1/\lambda to accommodate throughput, and at least the batch interval.
    fn set_next_write(&mut self, options: &ThrottleOptions) {
        let offset = std::cmp::max(
            Duration::from_micros(1_000_000 / options.throughput as u64),
            Duration::from_micros(options.batch_interval_us as u64),
        );
        self.next_write = Instant::now() + offset;
    }

    /// Select a fragment seq no and log position for writing, if possible.
    fn select_for_write<FP: FragmentPointer>(
        &mut self,
        options: &ThrottleOptions,
        manifest_manager: &(dyn ManifestPublisher<FP> + Sync),
        record_count: usize,
    ) -> Result<Option<FP>, Error> {
        if self.next_write > Instant::now() {
            return Ok(None);
        }
        if self.writers_active > 0 {
            return Ok(None);
        }
        let pointer = match manifest_manager.assign_timestamp(record_count) {
            Some(pointer) => pointer,
            None => {
                return Err(Error::LogFull);
            }
        };
        self.writers_active += 1;
        self.set_next_write(options);
        Ok(Some(pointer))
    }

    fn finish_write(&mut self) {
        self.writers_active -= 1;
    }
}

impl Drop for ManagerState {
    fn drop(&mut self) {
        for (_, notify, _) in std::mem::take(&mut self.enqueued).into_iter() {
            let _ = notify.send(Err(Error::LogContentionRetry));
        }
    }
}

/////////////////////////////////////////// BatchManager ///////////////////////////////////////////

pub struct BatchManager<FP: FragmentPointer, U: FragmentUploader<FP>> {
    options: LogWriterOptions,
    fragment_uploader: U,
    _fp_phantom: std::marker::PhantomData<FP>,
    state: Mutex<ManagerState>,
    write_finished: tokio::sync::Notify,
}

impl<FP: FragmentPointer, U: FragmentUploader<FP>> BatchManager<FP, U> {
    pub fn new(options: LogWriterOptions, fragment_uploader: U) -> Option<Self> {
        let next_write = Instant::now();
        Some(Self {
            fragment_uploader,
            _fp_phantom: std::marker::PhantomData,
            options,
            state: Mutex::new(ManagerState {
                backoff: false,
                next_write,
                writers_active: 0,
                enqueued: Vec::new(),
                tearing_down: false,
            }),
            write_finished: tokio::sync::Notify::new(),
        })
    }

    pub fn count_waiters(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.enqueued.len()
    }

    pub fn debug_dump(&self) -> String {
        let mut output = "[batch manager]\n".to_string();
        let state = self.state.lock().unwrap();
        output += &format!("backoff: {:?}\n", state.backoff);
        output += &format!("next_write: {:?}\n", state.next_write);
        output += &format!("writers_active: {:?}\n", state.writers_active);
        output += &format!("enqueued: {}\n", state.enqueued.len());
        output
    }
}

impl<FP: FragmentPointer, U: FragmentUploader<FP>> std::fmt::Debug for BatchManager<FP, U> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("BatchManager")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<FP: FragmentPointer, U: FragmentUploader<FP>> FragmentPublisher for BatchManager<FP, U> {
    type FragmentPointer = FP;

    /// Enqueue work to be published.
    async fn push_work(
        &self,
        messages: Vec<Vec<u8>>,
        tx: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        span: Span,
    ) {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();
        if state.tearing_down {
            let _ = tx.send(Err(Error::LogContentionRetry));
            self.write_finished.notify_one();
        } else if state.backoff {
            let _ = tx.send(Err(Error::Backoff));
            self.write_finished.notify_one();
        } else {
            state.enqueued.push((messages, tx, span));
        }
    }

    /// Take enqueued work to be published.
    async fn take_work(
        &self,
        manifest_manager: &(dyn ManifestPublisher<Self::FragmentPointer> + Sync),
    ) -> Result<
        Option<(
            Self::FragmentPointer,
            Vec<(
                Vec<Vec<u8>>,
                tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
                Span,
            )>,
        )>,
        Error,
    > {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();

        // We're shutting down.  Throw the work away.
        if state.tearing_down {
            self.write_finished.notify_one();
            return Ok(None);
        }

        // If there is no work, there is no notify.
        if state.enqueued.is_empty() {
            // No work, no notify.
            return Ok(None);
        }

        let mut split_off = 0usize;
        let mut acc_count = 0usize;
        let mut acc_bytes = 0usize;
        let mut did_split = false;
        // This loop has two sets of exit conditions that are identical, but switched on
        // `short_read`.
        for (batch, _, _) in state.enqueued.iter() {
            let cur_count = batch.len();
            let cur_bytes = batch.iter().map(|r| r.len()).sum::<usize>();
            if split_off > 0
                && acc_bytes + cur_bytes >= self.options.throttle_fragment.batch_size_bytes
            {
                did_split = true;
                break;
            }
            acc_count += cur_count;
            acc_bytes += cur_bytes;
            split_off += 1;
        }
        // If we haven't waited the batch interval since last write, and we didn't break early, wait for more data.
        if !did_split && state.next_write > Instant::now() {
            // This notify makes sure the background picks up the work and makes progress at end of
            // the batching interval.
            self.write_finished.notify_one();
            return Ok(None);
        }
        if split_off == 0 {
            // No work to do.
            self.write_finished.notify_one();
            return Ok(None);
        }
        let Some(pointer) =
            state.select_for_write(&self.options.throttle_fragment, manifest_manager, acc_count)?
        else {
            // Cannot yet select for write.  Notify will come from the timeout background is on.
            return Ok(None);
        };
        let mut work = std::mem::take(&mut state.enqueued);
        state.enqueued = work.split_off(split_off);
        if !state.enqueued.is_empty() {
            state.backoff = state
                .enqueued
                .iter()
                .map(|(recs, _, _)| recs.iter().map(|r| r.len()).sum::<usize>())
                .sum::<usize>()
                >= self.options.throttle_fragment.batch_size_bytes;
            self.write_finished.notify_one();
        } else {
            state.backoff = false;
        }
        Ok(Some((pointer, work)))
    }

    /// Finish the previous call to take_work.
    async fn finish_write(&self) {
        self.state.lock().unwrap().finish_write();
        self.write_finished.notify_one();
    }

    /// Wait until take_work might have work.
    async fn wait_for_writable(&self) {
        self.write_finished.notified().await;
    }

    /// How long to sleep until take work might have work.
    fn until_next_time(&self) -> Duration {
        // SAFETY(rescrv): Mutex poisoning.
        let state = self.state.lock().unwrap();
        let now = Instant::now();
        if now < state.next_write {
            state.next_write - now
        } else {
            Duration::ZERO
        }
    }

    /// upload a parquet fragment
    async fn upload_parquet(
        &self,
        pointer: &Self::FragmentPointer,
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
        epoch_micros: u64,
    ) -> Result<UploadResult, Error> {
        self.fragment_uploader
            .upload_parquet(pointer, messages, cmek, epoch_micros)
            .await
    }

    async fn read_json_file(&self, path: &str) -> Result<(Arc<Vec<u8>>, Option<ETag>), Error> {
        Ok(
            crate::interfaces::read_raw_bytes(path, self.fragment_uploader.storages().await)
                .await
                .map_err(Arc::new)?,
        )
    }

    async fn preferred_storage(&self) -> Storage {
        self.fragment_uploader.preferred_storage().await
    }

    async fn storages(&self) -> Vec<crate::StorageWrapper> {
        self.fragment_uploader.storages().await.to_vec()
    }

    /// Start shutting down.  The shutdown is split for historical and unprincipled reasons.
    fn shutdown_prepare(&self) {
        let enqueued = {
            let mut state = self.state.lock().unwrap();
            state.tearing_down = true;
            std::mem::take(&mut state.enqueued)
        };
        for (_, tx, _) in enqueued {
            let _ = tx.send(Err(Error::LogContentionRetry));
        }
    }

    /// Finish shutting down.
    fn shutdown_finish(&self) {
        self.write_finished.notify_one();
    }

    async fn write_garbage(
        &self,
        options: &crate::ThrottleOptions,
        existing: Option<&ETag>,
        garbage: &crate::Garbage,
    ) -> Result<Option<ETag>, Error> {
        let exp_backoff = crate::backoff::ExponentialBackoff::new(
            options.throughput as f64,
            options.headroom as f64,
        );
        let mut retry_count = 0;
        let preferred = self.fragment_uploader.preferred_storage_wrapper().await;
        loop {
            let path = Garbage::path(&preferred.prefix);
            let payload = serde_json::to_string(garbage)
                .map_err(|e| {
                    Error::CorruptManifest(format!("could not encode JSON garbage: {e:?}"))
                })?
                .into_bytes();
            let put_options = PutOptions::default().with_priority(StorageRequestPriority::P0);
            let put_options = if let Some(e_tag) = existing {
                put_options.with_mode(PutMode::IfMatch(e_tag.clone()))
            } else {
                put_options.with_mode(PutMode::IfNotExist)
            };
            match preferred
                .storage
                .put_bytes(&path, payload, put_options)
                .await
            {
                Ok(e_tag) => return Ok(e_tag),
                Err(StorageError::Precondition { path: _, source: _ }) => {
                    return Err(Error::LogContentionFailure);
                }
                Err(e) => {
                    tracing::error!("error uploading garbage: {e:?}");
                    let backoff = exp_backoff.next();
                    if backoff > std::time::Duration::from_secs(60) || retry_count >= 3 {
                        return Err(Arc::new(e).into());
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
            retry_count += 1;
        }
    }

    async fn reset_garbage(
        &self,
        options: &crate::ThrottleOptions,
        e_tag: &ETag,
    ) -> Result<(), Error> {
        let empty = crate::Garbage::empty();
        self.write_garbage(options, Some(e_tag), &empty).await?;
        Ok(())
    }

    async fn cursors(&self, options: CursorStoreOptions) -> CursorStore {
        let storage = Arc::new(self.fragment_uploader.preferred_storage().await);
        let prefix = self.fragment_uploader.preferred_prefix().await;
        CursorStore::new(options, storage, prefix, "batch_manager".to_string())
    }
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(options, storage, messages))]
pub async fn upload_parquet(
    options: &LogWriterOptions,
    storage: &Storage,
    prefix: &str,
    fragment_identifier: FragmentIdentifier,
    log_position: Option<LogPosition>,
    messages: Vec<Vec<u8>>,
    cmek: Option<Cmek>,
    epoch_micros: u64,
) -> Result<(String, Setsum, usize), Error> {
    // Upload the log.
    let unprefixed_path = crate::unprefixed_fragment_path(fragment_identifier);
    let path = format!("{prefix}/{unprefixed_path}");
    let exp_backoff: ExponentialBackoff = options.throttle_fragment.into();
    let start = Instant::now();
    let (buffer, setsum) = crate::writer::construct_parquet(log_position, &messages, epoch_micros)?;
    let mut put_options = PutOptions::default()
        .with_priority(StorageRequestPriority::P0)
        .with_mode(PutMode::IfNotExist);
    if let Some(cmek) = cmek {
        put_options = put_options.with_cmek(cmek);
    }
    loop {
        tracing::info!("upload_parquet: {:?} with {} bytes", path, buffer.len());
        // NOTE(rescrv):  This match block has been thoroughly reasoned through within the
        // `bootstrap` call above.  Don't change the error handling here without re-reasoning
        // there.
        match storage
            .put_bytes(&path, buffer.clone(), put_options.clone())
            .await
        {
            Ok(_) => {
                return Ok((unprefixed_path, setsum, buffer.len()));
            }
            // NOTE(sicheng): Permission denied requests should continue to fail if retried
            Err(err @ StorageError::PermissionDenied { .. }) => {
                return Err(Error::StorageError(Arc::new(err)));
            }
            Err(StorageError::Precondition { path: _, source: _ }) => {
                return Err(Error::LogContentionFailure);
            }
            Err(err) => {
                tracing::error!(
                    error.message = err.to_string(),
                    "failed to upload parquet, backing off"
                );
                // NOTE(sicheng): The frontend will fail the request on its end if we retry for too long here
                // TODO(sicheng): Organize the magic numbers in the code at one place
                if start.elapsed() > Duration::from_secs(20) {
                    return Err(Error::StorageError(Arc::new(err)));
                }
                let mut backoff = exp_backoff.next();
                if backoff > Duration::from_secs(10) {
                    backoff = Duration::from_secs(10);
                }
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chroma_storage::s3_client_for_test_with_new_bucket;

    use super::*;
    use crate::interfaces::s3::manifest_manager::ManifestManager;
    use crate::interfaces::s3::S3FragmentUploader;
    use crate::{FragmentSeqNo, LogWriterOptions, SnapshotOptions, ThrottleOptions};

    #[tokio::test]
    async fn test_k8s_integration_batches() {
        let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
        let prefix = "test-batches-prefix".to_string();
        let options = LogWriterOptions {
            throttle_fragment: ThrottleOptions {
                throughput: 100,
                headroom: 1,
                batch_size_bytes: 4,
                batch_interval_us: 1_000_000,
            },
            ..Default::default()
        };
        let fragment_uploader = S3FragmentUploader::new(
            options.clone(),
            Storage::clone(&*storage),
            prefix.clone(),
            Arc::new(()),
        );
        let batch_manager = BatchManager::new(options, fragment_uploader).unwrap();
        ManifestManager::initialize(
            &LogWriterOptions::default(),
            &storage,
            &prefix,
            "initializer",
        )
        .await
        .unwrap();
        let manifest_manager = ManifestManager::new(
            ThrottleOptions::default(),
            SnapshotOptions::default(),
            storage,
            prefix.clone(),
            "writer".to_string(),
            Arc::new(()),
            Arc::new(()),
        )
        .await
        .unwrap();
        let (tx, _rx1) = tokio::sync::oneshot::channel();
        batch_manager
            .push_work(vec![vec![1]], tx, tracing::Span::current())
            .await;
        let (tx, _rx2) = tokio::sync::oneshot::channel();
        batch_manager
            .push_work(vec![vec![2, 3]], tx, tracing::Span::current())
            .await;
        let (tx, _rx3) = tokio::sync::oneshot::channel();
        batch_manager
            .push_work(vec![vec![4, 5, 6]], tx, tracing::Span::current())
            .await;
        let ((seq_no, log_position), work) = batch_manager
            .take_work(&manifest_manager)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seq_no, FragmentSeqNo::from_u64(1));
        assert_eq!(log_position.offset(), 1);
        assert_eq!(2, work.len());
        // Check batch 1
        assert_eq!(vec![vec![1]], work[0].0);
        // Check batch 2
        assert_eq!(vec![vec![2, 3]], work[1].0);
    }
}
