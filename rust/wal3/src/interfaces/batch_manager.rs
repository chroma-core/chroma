use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, ETag, PutMode, PutOptions, Storage, StorageError,
};
use chroma_types::Cmek;
use setsum::Setsum;

use crate::backoff::ExponentialBackoff;
use crate::interfaces::{
    AppendWork, FragmentPointer, FragmentPublisher, ManifestPublisher, UploadResult,
};
use crate::{Error, FragmentIdentifier, Garbage, LogPosition, LogWriterOptions, ThrottleOptions};

use super::FragmentUploader;

/////////////////////////////////////////// ManagerState ///////////////////////////////////////////

/// ManagerState captures the state necessary to batch manifests.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
struct ManagerState {
    backoff: bool,
    next_write: Instant,
    writers_active: usize,
    enqueued: Vec<AppendWork>,
    admission_metadata: Vec<Vec<Arc<[u8]>>>,
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

fn required_fragment_start_for_selected(selected: &[AppendWork]) -> Option<LogPosition> {
    let mut required_fragment_start = None;
    for work in selected {
        let Some(next_required_fragment_start) = work.required_fragment_start() else {
            continue;
        };
        if let Some(required_fragment_start) = required_fragment_start {
            debug_assert_eq!(required_fragment_start, next_required_fragment_start);
        } else {
            required_fragment_start = Some(next_required_fragment_start);
        }
    }
    required_fragment_start
}

fn count_compatible_required_fragment_starts(selected: &[AppendWork]) -> usize {
    let mut required_fragment_start = None;
    for (index, work) in selected.iter().enumerate() {
        let Some(next_required_fragment_start) = work.required_fragment_start() else {
            continue;
        };
        if let Some(required_fragment_start) = required_fragment_start {
            if required_fragment_start != next_required_fragment_start {
                tracing::info!(
                    ?required_fragment_start,
                    ?next_required_fragment_start,
                    selected_work_count = selected.len(),
                    compatible_work_count = index,
                    "splitting wal3 batch on incompatible required_fragment_start"
                );
                return index;
            }
        } else {
            required_fragment_start = Some(next_required_fragment_start);
        }
    }
    selected.len()
}

fn take_selected_work(state: &mut ManagerState, split_off: usize) -> Vec<AppendWork> {
    let mut work = std::mem::take(&mut state.enqueued);
    state.enqueued = work.split_off(split_off);
    let mut admission_metadata = std::mem::take(&mut state.admission_metadata);
    state.admission_metadata = admission_metadata.split_off(split_off);
    debug_assert_eq!(work.len(), admission_metadata.len());
    debug_assert_eq!(state.enqueued.len(), state.admission_metadata.len());
    work
}

fn refresh_backoff_after_split(state: &mut ManagerState, options: &LogWriterOptions) -> bool {
    if !state.enqueued.is_empty() {
        state.backoff = state
            .enqueued
            .iter()
            .map(AppendWork::byte_count)
            .sum::<usize>()
            >= options.throttle_fragment.batch_size_bytes;
        true
    } else {
        state.backoff = false;
        false
    }
}

fn reject_selected_work(work: Vec<AppendWork>, err: Error) {
    for work in work {
        let _ = work.tx.send(Err(err.clone()));
    }
}

impl Drop for ManagerState {
    fn drop(&mut self) {
        for work in std::mem::take(&mut self.enqueued).into_iter() {
            let _ = work.tx.send(Err(Error::LogContentionRetry));
        }
        self.admission_metadata.clear();
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
                admission_metadata: Vec::new(),
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
    async fn push_work(&self, work: AppendWork) {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();
        if state.tearing_down {
            let _ = work.tx.send(Err(Error::LogContentionRetry));
            self.write_finished.notify_one();
        } else if state.backoff {
            let _ = work.tx.send(Err(Error::Backoff));
            self.write_finished.notify_one();
        } else {
            if let Some(admission_predicate) = work
                .options
                .as_ref()
                .and_then(|options| options.admission_predicate.as_ref())
            {
                if !admission_predicate(&state.admission_metadata) {
                    let _ = work.tx.send(Err(Error::AdmissionRejected));
                    self.write_finished.notify_one();
                    return;
                }
            }
            state.admission_metadata.push(work.admission_metadata());
            state.enqueued.push(work);
        }
    }

    /// Take enqueued work to be published.
    async fn take_work(
        &self,
        manifest_manager: &(dyn ManifestPublisher<Self::FragmentPointer> + Sync),
    ) -> Result<Option<(Self::FragmentPointer, Option<LogPosition>, Vec<AppendWork>)>, Error> {
        // SAFETY(rescrv): Mutex poisoning.
        let mut state = self.state.lock().unwrap();

        // We're shutting down.  Throw the work away.
        if state.tearing_down {
            state.enqueued.clear();
            state.admission_metadata.clear();
            self.write_finished.notify_one();
            return Ok(None);
        }

        // If there is no work, there is no notify.
        if state.enqueued.is_empty() {
            // No work, no notify.
            return Ok(None);
        }
        debug_assert_eq!(state.enqueued.len(), state.admission_metadata.len());

        let mut split_off = 0usize;
        let mut acc_count = 0usize;
        let mut acc_bytes = 0usize;
        let mut did_split = false;
        // This loop has two sets of exit conditions that are identical, but switched on
        // `short_read`.
        for work in state.enqueued.iter() {
            let cur_count = work.record_count();
            let cur_bytes = work.byte_count();
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
        let compatible_split_off =
            count_compatible_required_fragment_starts(&state.enqueued[..split_off]);
        if compatible_split_off < split_off {
            split_off = compatible_split_off;
            acc_count = state.enqueued[..split_off]
                .iter()
                .map(AppendWork::record_count)
                .sum();
        }
        let required_fragment_start =
            required_fragment_start_for_selected(&state.enqueued[..split_off]);
        let Some(pointer) =
            state.select_for_write(&self.options.throttle_fragment, manifest_manager, acc_count)?
        else {
            // Cannot yet select for write.  Notify will come from the timeout background is on.
            return Ok(None);
        };
        if let (Some(required_fragment_start), Some(assigned_fragment_start)) =
            (required_fragment_start, pointer.fragment_start())
        {
            if required_fragment_start != assigned_fragment_start {
                tracing::info!(
                    ?required_fragment_start,
                    ?assigned_fragment_start,
                    selected_work_count = split_off,
                    "selected wal3 batch missed required_fragment_start"
                );
                let work = take_selected_work(&mut state, split_off);
                state.finish_write();
                if refresh_backoff_after_split(&mut state, &self.options) {
                    self.write_finished.notify_one();
                }
                reject_selected_work(work, Error::LogContentionRetry);
                return Ok(None);
            }
        }
        let work = take_selected_work(&mut state, split_off);
        if refresh_backoff_after_split(&mut state, &self.options) {
            self.write_finished.notify_one();
        }
        Ok(Some((pointer, required_fragment_start, work)))
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
        let preferred = self.fragment_uploader.preferred_storage_wrapper().await;
        let path = crate::fragment_path(&preferred.prefix, path);
        Ok(preferred
            .storage
            .get_with_e_tag(
                &path,
                chroma_storage::GetOptions::new(StorageRequestPriority::P0),
            )
            .await
            .map_err(Arc::new)?)
    }

    async fn preferred_storage(&self) -> Storage {
        self.fragment_uploader.preferred_storage().await
    }

    async fn preferred_prefix(&self) -> String {
        self.fragment_uploader.preferred_prefix().await
    }

    async fn storages(&self) -> Vec<crate::StorageWrapper> {
        self.fragment_uploader.storages().await.to_vec()
    }

    /// Start shutting down.  The shutdown is split for historical and unprincipled reasons.
    fn shutdown_prepare(&self) {
        let enqueued = {
            let mut state = self.state.lock().unwrap();
            state.tearing_down = true;
            state.admission_metadata.clear();
            std::mem::take(&mut state.enqueued)
        };
        for work in enqueued {
            let _ = work.tx.send(Err(Error::LogContentionRetry));
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
                Err(StorageError::AlreadyExists { path: _, source: _ }) => {
                    return Err(Error::LogContentionFailure);
                }
                Err(StorageError::Precondition { path: _, source: _ }) => {
                    return Err(Error::LogContentionFailure);
                }
                Err(StorageError::NotFound { path: _, source: _ }) => {
                    // NotFound means another process deleted gc/GARBAGE between our
                    // load (which obtained the ETag) and this conditional put.  Treat
                    // it as contention so callers retry with a fresh load.
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
        match self.write_garbage(options, Some(e_tag), &empty).await {
            Ok(_) => Ok(()),
            Err(Error::LogContentionFailure) => {
                // The GARBAGE file was modified or deleted by another process between
                // our load and this reset.  Either the file was deleted (nothing to
                // reset) or overwritten with new content from a concurrent GC cycle
                // (the new content will be processed in a future cycle).  Both cases
                // are safe to treat as success.
                tracing::info!("garbage reset skipped: file was concurrently modified or deleted");
                Ok(())
            }
            Err(err) => {
                tracing::error!(error =% err, "could not write garbage");
                Err(err)
            }
        }
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
        tracing::info!(path = %path, bytes_uploaded = %buffer.len(), num_records = %messages.len(), "upload_parquet");
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
            Err(StorageError::AlreadyExists { path: _, source: _ })
            | Err(StorageError::Precondition { path: _, source: _ }) => {
                // NOTE(rescrv):  It's gotta be a retry here because there was no write; the data
                // is safe to retry; percolates as an error to the user otherwise when the requests
                // are retryable.
                return Err(Error::LogContentionRetry);
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

    use chroma_storage::{s3_client_for_test_with_new_bucket, test_storage, PutOptions};

    use super::*;
    use crate::interfaces::s3::manifest_manager::ManifestManager;
    use crate::interfaces::s3::S3FragmentUploader;
    use crate::{
        AppendOptions, FragmentSeqNo, FragmentUuid, LogWriterOptions, Manifest, ManifestAndWitness,
        ManifestWitness, Snapshot, SnapshotOptions, SnapshotPointer, StorageWrapper,
        ThrottleOptions,
    };

    struct NoopUploader;

    #[async_trait::async_trait]
    impl FragmentUploader<FragmentUuid> for NoopUploader {
        async fn upload_parquet(
            &self,
            _pointer: &FragmentUuid,
            _messages: Vec<Vec<u8>>,
            _cmek: Option<Cmek>,
            _epoch_micros: u64,
        ) -> Result<UploadResult, Error> {
            unreachable!("upload_parquet is not used in admission tests")
        }

        async fn preferred_storage(&self) -> Storage {
            unreachable!("preferred_storage is not used in admission tests")
        }

        async fn preferred_prefix(&self) -> String {
            unreachable!("preferred_prefix is not used in admission tests")
        }

        async fn preferred_storage_wrapper(&self) -> &StorageWrapper {
            unreachable!("preferred_storage_wrapper is not used in admission tests")
        }

        async fn storages(&self) -> &[StorageWrapper] {
            unreachable!("storages is not used in admission tests")
        }
    }

    fn admission_batch_manager() -> BatchManager<FragmentUuid, NoopUploader> {
        BatchManager::new(LogWriterOptions::default(), NoopUploader).unwrap()
    }

    fn admission_metadata(metadata: &[&str]) -> Vec<Arc<[u8]>> {
        metadata
            .iter()
            .map(|metadata| Arc::<[u8]>::from(metadata.as_bytes()))
            .collect()
    }

    fn append_options(metadata: &[&str]) -> AppendOptions {
        AppendOptions::new(admission_metadata(metadata))
    }

    struct NoopS3Uploader;

    #[async_trait::async_trait]
    impl FragmentUploader<(FragmentSeqNo, LogPosition)> for NoopS3Uploader {
        async fn upload_parquet(
            &self,
            _pointer: &(FragmentSeqNo, LogPosition),
            _messages: Vec<Vec<u8>>,
            _cmek: Option<Cmek>,
            _epoch_micros: u64,
        ) -> Result<UploadResult, Error> {
            unreachable!("upload_parquet is not used in required-start selection tests")
        }

        async fn preferred_storage(&self) -> Storage {
            unreachable!("preferred_storage is not used in required-start selection tests")
        }

        async fn preferred_prefix(&self) -> String {
            unreachable!("preferred_prefix is not used in required-start selection tests")
        }

        async fn preferred_storage_wrapper(&self) -> &StorageWrapper {
            unreachable!("preferred_storage_wrapper is not used in required-start selection tests")
        }

        async fn storages(&self) -> &[StorageWrapper] {
            unreachable!("storages is not used in required-start selection tests")
        }
    }

    fn s3_batch_manager() -> BatchManager<(FragmentSeqNo, LogPosition), NoopS3Uploader> {
        BatchManager::new(LogWriterOptions::default(), NoopS3Uploader).unwrap()
    }

    fn immediate_s3_batch_manager() -> BatchManager<(FragmentSeqNo, LogPosition), NoopS3Uploader> {
        let options = LogWriterOptions {
            throttle_fragment: ThrottleOptions {
                throughput: 2_000_000,
                batch_interval_us: 0,
                ..ThrottleOptions::default()
            },
            ..LogWriterOptions::default()
        };
        BatchManager::new(options, NoopS3Uploader).unwrap()
    }

    async fn enqueue(
        batch_manager: &BatchManager<FragmentUuid, NoopUploader>,
        message: Vec<u8>,
        options: Option<AppendOptions>,
    ) -> tokio::sync::oneshot::Receiver<Result<LogPosition, Error>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        batch_manager
            .push_work(AppendWork::new(
                vec![message],
                options,
                tx,
                tracing::Span::current(),
            ))
            .await;
        rx
    }

    async fn enqueue_s3(
        batch_manager: &BatchManager<(FragmentSeqNo, LogPosition), NoopS3Uploader>,
        message: Vec<u8>,
        options: Option<AppendOptions>,
    ) -> tokio::sync::oneshot::Receiver<Result<LogPosition, Error>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        batch_manager
            .push_work(AppendWork::new(
                vec![message],
                options,
                tx,
                tracing::Span::current(),
            ))
            .await;
        rx
    }

    async fn enqueue_s3_with_required_start(
        batch_manager: &BatchManager<(FragmentSeqNo, LogPosition), NoopS3Uploader>,
        message: &str,
        required_fragment_start: Option<u64>,
    ) -> tokio::sync::oneshot::Receiver<Result<LogPosition, Error>> {
        let options = required_fragment_start.map(|offset| {
            append_options(&[message])
                .with_required_fragment_start(LogPosition::from_offset(offset))
        });
        enqueue_s3(batch_manager, Vec::from(message), options).await
    }

    async fn take_s3_work(
        batch_manager: &BatchManager<(FragmentSeqNo, LogPosition), NoopS3Uploader>,
        assigned_fragment_start: u64,
    ) -> (
        (FragmentSeqNo, LogPosition),
        Option<LogPosition>,
        Vec<AppendWork>,
    ) {
        batch_manager
            .take_work(&FixedS3ManifestPublisher {
                assigned_fragment_start: LogPosition::from_offset(assigned_fragment_start),
            })
            .await
            .expect("required-start selection should not fail take_work")
            .expect("compatible prefix should be selected")
    }

    fn assert_s3_work(
        selected: (
            (FragmentSeqNo, LogPosition),
            Option<LogPosition>,
            Vec<AppendWork>,
        ),
        assigned_fragment_start: u64,
        required_fragment_start: Option<u64>,
        messages: &[&str],
    ) -> Vec<AppendWork> {
        let ((seq_no, log_position), selected_required_fragment_start, work) = selected;
        assert_eq!(FragmentSeqNo::BEGIN, seq_no);
        assert_eq!(
            LogPosition::from_offset(assigned_fragment_start),
            log_position
        );
        assert_eq!(
            required_fragment_start.map(LogPosition::from_offset),
            selected_required_fragment_start
        );
        assert_eq!(messages.len(), work.len());
        let selected_messages: Vec<Vec<Vec<u8>>> =
            work.iter().map(|work| work.messages.clone()).collect();
        let expected_messages: Vec<Vec<Vec<u8>>> = messages
            .iter()
            .map(|message| vec![Vec::from(*message)])
            .collect();
        assert_eq!(expected_messages, selected_messages);
        work
    }

    async fn commit_s3_work(
        batch_manager: &BatchManager<(FragmentSeqNo, LogPosition), NoopS3Uploader>,
        mut log_position: LogPosition,
        work: Vec<AppendWork>,
    ) {
        for work in work {
            let record_count = work.record_count();
            assert!(work.tx.send(Ok(log_position)).is_ok());
            log_position += record_count;
        }
        batch_manager.finish_write().await;
    }

    async fn expect_committed(
        rx: tokio::sync::oneshot::Receiver<Result<LogPosition, Error>>,
        log_position: u64,
    ) {
        assert_eq!(
            LogPosition::from_offset(log_position),
            rx.await
                .expect("append should receive a result")
                .expect("append should commit")
        );
    }

    struct FixedS3ManifestPublisher {
        assigned_fragment_start: LogPosition,
    }

    #[async_trait::async_trait]
    impl ManifestPublisher<(FragmentSeqNo, LogPosition)> for FixedS3ManifestPublisher {
        async fn recover(&mut self) -> Result<(), Error> {
            unreachable!("recover is not used in required-start selection tests")
        }

        async fn manifest_and_witness(&self) -> Result<ManifestAndWitness, Error> {
            unreachable!("manifest_and_witness is not used in required-start selection tests")
        }

        fn assign_timestamp(&self, _record_count: usize) -> Option<(FragmentSeqNo, LogPosition)> {
            Some((FragmentSeqNo::BEGIN, self.assigned_fragment_start))
        }

        async fn publish_fragment(
            &self,
            _pointer: &(FragmentSeqNo, LogPosition),
            _path: &str,
            _messages_len: u64,
            _num_bytes: u64,
            _setsum: Setsum,
            _required_fragment_start: Option<LogPosition>,
            _successful_regions: &[String],
        ) -> Result<LogPosition, Error> {
            unreachable!("publish_fragment is not used in required-start selection tests")
        }

        async fn garbage_applies_cleanly(&self, _garbage: &Garbage) -> Result<bool, Error> {
            unreachable!("garbage_applies_cleanly is not used in required-start selection tests")
        }

        async fn apply_garbage(&self, _garbage: Garbage) -> Result<(), Error> {
            unreachable!("apply_garbage is not used in required-start selection tests")
        }

        async fn compute_garbage(
            &self,
            _options: &crate::GarbageCollectionOptions,
            _first_to_keep: LogPosition,
        ) -> Result<Option<Garbage>, Error> {
            unreachable!("compute_garbage is not used in required-start selection tests")
        }

        async fn snapshot_load(
            &self,
            _pointer: &SnapshotPointer,
        ) -> Result<Option<Snapshot>, Error> {
            unreachable!("snapshot_load is not used in required-start selection tests")
        }

        async fn snapshot_install(&self, _snapshot: &Snapshot) -> Result<SnapshotPointer, Error> {
            unreachable!("snapshot_install is not used in required-start selection tests")
        }

        async fn manifest_head(&self, _witness: &ManifestWitness) -> Result<bool, Error> {
            unreachable!("manifest_head is not used in required-start selection tests")
        }

        async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error> {
            unreachable!("manifest_load is not used in required-start selection tests")
        }

        fn shutdown(&self) {}

        async fn destroy(&self) -> Result<(), Error> {
            unreachable!("destroy is not used in required-start selection tests")
        }

        async fn load_intrinsic_cursor(&self) -> Result<Option<LogPosition>, Error> {
            unreachable!("load_intrinsic_cursor is not used in required-start selection tests")
        }
    }

    #[tokio::test]
    async fn admission_predicate_sees_earlier_metadata() {
        let batch_manager = admission_batch_manager();
        let _first_rx = enqueue(
            &batch_manager,
            Vec::from("first"),
            Some(append_options(&["metadata-1", "metadata-1b"])),
        )
        .await;

        let observed = Arc::new(std::sync::Mutex::new(Vec::<Vec<Vec<u8>>>::new()));
        let observed_clone = Arc::clone(&observed);
        let predicate = Arc::new(move |earlier_metadata: &[Vec<Arc<[u8]>>]| {
            *observed_clone.lock().unwrap() = earlier_metadata
                .iter()
                .map(|entry_metadata| {
                    entry_metadata
                        .iter()
                        .map(|metadata| metadata.as_ref().to_vec())
                        .collect()
                })
                .collect();
            true
        });
        let _second_rx = enqueue(
            &batch_manager,
            Vec::from("second"),
            Some(append_options(&["metadata-2"]).with_admission_predicate(predicate)),
        )
        .await;

        assert_eq!(
            *observed.lock().unwrap(),
            vec![vec![Vec::from("metadata-1"), Vec::from("metadata-1b")]]
        );
        assert_eq!(2, batch_manager.count_waiters());
    }

    #[tokio::test]
    async fn admission_predicate_does_not_see_candidate_metadata() {
        let batch_manager = admission_batch_manager();
        let predicate = Arc::new(move |earlier_metadata: &[Vec<Arc<[u8]>>]| {
            !earlier_metadata
                .iter()
                .flatten()
                .any(|metadata| metadata.as_ref() == b"candidate")
        });
        let mut rx = enqueue(
            &batch_manager,
            Vec::from("candidate"),
            Some(append_options(&["candidate"]).with_admission_predicate(predicate)),
        )
        .await;

        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(1, batch_manager.count_waiters());
    }

    #[tokio::test]
    async fn admission_rejection_is_non_fatal() {
        let batch_manager = admission_batch_manager();
        let mut first_rx = enqueue(
            &batch_manager,
            Vec::from("first"),
            Some(append_options(&["first"])),
        )
        .await;
        let rejecting_predicate = Arc::new(|_earlier_metadata: &[Vec<Arc<[u8]>>]| false);
        let rejected_rx = enqueue(
            &batch_manager,
            Vec::from("reject"),
            Some(append_options(&["reject"]).with_admission_predicate(rejecting_predicate)),
        )
        .await;
        let mut third_rx = enqueue(
            &batch_manager,
            Vec::from("third"),
            Some(append_options(&["third"])),
        )
        .await;

        let rejected = rejected_rx.await.unwrap();
        assert!(matches!(rejected, Err(Error::AdmissionRejected)));
        assert!(matches!(
            first_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            third_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(2, batch_manager.count_waiters());
    }

    #[tokio::test]
    async fn normal_writes_contribute_empty_admission_metadata() {
        let batch_manager = admission_batch_manager();
        let _first_rx = enqueue(&batch_manager, Vec::from("normal"), None).await;

        let observed = Arc::new(std::sync::Mutex::new(Vec::<Vec<Vec<u8>>>::new()));
        let observed_clone = Arc::clone(&observed);
        let predicate = Arc::new(move |earlier_metadata: &[Vec<Arc<[u8]>>]| {
            *observed_clone.lock().unwrap() = earlier_metadata
                .iter()
                .map(|entry_metadata| {
                    entry_metadata
                        .iter()
                        .map(|metadata| metadata.as_ref().to_vec())
                        .collect()
                })
                .collect();
            true
        });
        let _second_rx = enqueue(
            &batch_manager,
            Vec::from("conditional"),
            Some(append_options(&["conditional"]).with_admission_predicate(predicate)),
        )
        .await;

        assert_eq!(*observed.lock().unwrap(), vec![Vec::<Vec<u8>>::new()]);
        assert_eq!(2, batch_manager.count_waiters());
    }

    #[tokio::test]
    async fn mismatched_required_fragment_starts_split_selected_batch() {
        let batch_manager = immediate_s3_batch_manager();
        let first_rx = enqueue_s3_with_required_start(&batch_manager, "first", Some(10)).await;
        let mut second_rx =
            enqueue_s3_with_required_start(&batch_manager, "second", Some(11)).await;

        let work = assert_s3_work(
            take_s3_work(&batch_manager, 10).await,
            10,
            Some(10),
            &["first"],
        );
        assert_eq!(1, batch_manager.count_waiters());
        assert!(matches!(
            second_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        commit_s3_work(&batch_manager, LogPosition::from_offset(10), work).await;
        expect_committed(first_rx, 10).await;

        let work = assert_s3_work(
            take_s3_work(&batch_manager, 11).await,
            11,
            Some(11),
            &["second"],
        );
        assert_eq!(0, batch_manager.count_waiters());
        commit_s3_work(&batch_manager, LogPosition::from_offset(11), work).await;
        expect_committed(second_rx, 11).await;
    }

    #[tokio::test]
    async fn compatible_required_fragment_start_prefix_commits_before_requeue() {
        let batch_manager = immediate_s3_batch_manager();
        let first_rx = enqueue_s3_with_required_start(&batch_manager, "first", Some(10)).await;
        let second_rx = enqueue_s3_with_required_start(&batch_manager, "second", Some(10)).await;
        let third_rx = enqueue_s3_with_required_start(&batch_manager, "third", Some(20)).await;

        let work = assert_s3_work(
            take_s3_work(&batch_manager, 10).await,
            10,
            Some(10),
            &["first", "second"],
        );
        assert_eq!(1, batch_manager.count_waiters());
        commit_s3_work(&batch_manager, LogPosition::from_offset(10), work).await;
        expect_committed(first_rx, 10).await;
        expect_committed(second_rx, 11).await;

        let work = assert_s3_work(
            take_s3_work(&batch_manager, 20).await,
            20,
            Some(20),
            &["third"],
        );
        assert_eq!(0, batch_manager.count_waiters());
        commit_s3_work(&batch_manager, LogPosition::from_offset(20), work).await;
        expect_committed(third_rx, 20).await;
    }

    #[tokio::test]
    async fn none_required_fragment_start_between_mismatches_stays_with_prefix() {
        let batch_manager = immediate_s3_batch_manager();
        let first_rx = enqueue_s3_with_required_start(&batch_manager, "first", Some(10)).await;
        let second_rx = enqueue_s3_with_required_start(&batch_manager, "second", None).await;
        let third_rx = enqueue_s3_with_required_start(&batch_manager, "third", Some(20)).await;

        let work = assert_s3_work(
            take_s3_work(&batch_manager, 10).await,
            10,
            Some(10),
            &["first", "second"],
        );
        assert_eq!(1, batch_manager.count_waiters());
        commit_s3_work(&batch_manager, LogPosition::from_offset(10), work).await;
        expect_committed(first_rx, 10).await;
        expect_committed(second_rx, 11).await;

        let work = assert_s3_work(
            take_s3_work(&batch_manager, 20).await,
            20,
            Some(20),
            &["third"],
        );
        assert_eq!(0, batch_manager.count_waiters());
        commit_s3_work(&batch_manager, LogPosition::from_offset(20), work).await;
        expect_committed(third_rx, 20).await;
    }

    #[tokio::test]
    async fn s3_required_fragment_start_mismatch_rejects_before_upload_selection() {
        let batch_manager = s3_batch_manager();
        let rx = enqueue_s3(
            &batch_manager,
            Vec::from("record"),
            Some(
                append_options(&["record"])
                    .with_required_fragment_start(LogPosition::from_offset(42)),
            ),
        )
        .await;
        let manifest = FixedS3ManifestPublisher {
            assigned_fragment_start: LogPosition::from_offset(41),
        };

        let selected = batch_manager
            .take_work(&manifest)
            .await
            .expect("required-start mismatch should not fail take_work");

        assert!(selected.is_none());
        assert_eq!(0, batch_manager.count_waiters());
        assert!(matches!(
            rx.await.expect("append should receive a result"),
            Err(Error::LogContentionRetry)
        ));
    }

    #[tokio::test]
    async fn test_k8s_integration_upload_parquet_returns_retry_on_already_exists() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let prefix = "test-upload-parquet-retry";
        let options = LogWriterOptions::default();
        let fragment_identifier = FragmentIdentifier::SeqNo(FragmentSeqNo::from_u64(42));
        let unprefixed_path = crate::unprefixed_fragment_path(fragment_identifier);
        let path = format!("{prefix}/{unprefixed_path}");
        // Pre-populate the path so that IfNotExist triggers AlreadyExists.
        storage
            .put_bytes(
                &path,
                b"pre-existing data".to_vec(),
                chroma_storage::PutOptions::default(),
            )
            .await
            .expect("pre-population should succeed");
        let messages = vec![vec![1, 2, 3]];
        let result = upload_parquet(
            &options,
            &storage,
            prefix,
            fragment_identifier,
            Some(LogPosition::from_offset(1)),
            messages,
            None,
            1_000_000,
        )
        .await;
        let err = result.expect_err("upload_parquet should fail with LogContentionRetry");
        println!("upload_parquet_returns_retry_on_already_exists: err={err:?}");
        assert!(
            matches!(err, Error::LogContentionRetry),
            "expected LogContentionRetry, got {err:?}"
        );
    }

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
        let options1 = append_options(&["metadata-1"]);
        batch_manager
            .push_work(AppendWork::new(
                vec![vec![1]],
                Some(options1.clone()),
                tx,
                tracing::Span::current(),
            ))
            .await;
        let (tx, _rx2) = tokio::sync::oneshot::channel();
        batch_manager
            .push_work(AppendWork::new(
                vec![vec![2, 3]],
                None,
                tx,
                tracing::Span::current(),
            ))
            .await;
        let (tx, _rx3) = tokio::sync::oneshot::channel();
        batch_manager
            .push_work(AppendWork::new(
                vec![vec![4, 5, 6]],
                None,
                tx,
                tracing::Span::current(),
            ))
            .await;
        let ((seq_no, log_position), required_fragment_start, work) = batch_manager
            .take_work(&manifest_manager)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seq_no, FragmentSeqNo::from_u64(1));
        assert_eq!(log_position.offset(), 1);
        assert_eq!(None, required_fragment_start);
        assert_eq!(2, work.len());
        // Check batch 1
        assert_eq!(vec![vec![1]], work[0].messages);
        assert_eq!(
            Some(options1.admission_metadata.as_slice()),
            work[0]
                .options
                .as_ref()
                .map(|options| options.admission_metadata.as_slice())
        );
        // Check batch 2
        assert_eq!(vec![vec![2, 3]], work[1].messages);
        assert!(work[1].options.is_none());
    }

    struct DualStorageUploader {
        preferred: usize,
        storages: Arc<Vec<StorageWrapper>>,
    }

    #[async_trait::async_trait]
    impl FragmentUploader<FragmentUuid> for DualStorageUploader {
        async fn upload_parquet(
            &self,
            _pointer: &FragmentUuid,
            _messages: Vec<Vec<u8>>,
            _cmek: Option<Cmek>,
            _epoch_micros: u64,
        ) -> Result<UploadResult, Error> {
            unreachable!("upload_parquet is not used in this test")
        }

        async fn preferred_storage(&self) -> Storage {
            self.storages[self.preferred].storage.clone()
        }

        async fn preferred_prefix(&self) -> String {
            self.storages[self.preferred].prefix.clone()
        }

        async fn preferred_storage_wrapper(&self) -> &StorageWrapper {
            &self.storages[self.preferred]
        }

        async fn storages(&self) -> &[StorageWrapper] {
            &self.storages
        }
    }

    #[tokio::test]
    async fn test_read_json_file_uses_only_preferred_storage() {
        let (_preferred_dir, preferred_storage) = test_storage();
        let (_replica_dir, replica_storage) = test_storage();
        let prefix = "test-read-json-file-preferred".to_string();
        let path = format!("{prefix}/gc/GARBAGE");

        replica_storage
            .put_bytes(
                &path,
                br#"{"first_to_keep":1}"#.to_vec(),
                PutOptions::default(),
            )
            .await
            .expect("write to replica storage");

        let fragment_uploader = DualStorageUploader {
            preferred: 0,
            storages: Arc::new(vec![
                StorageWrapper::new(
                    "preferred".to_string(),
                    preferred_storage.clone(),
                    prefix.clone(),
                ),
                StorageWrapper::new("replica".to_string(), replica_storage, prefix),
            ]),
        };
        let batch_manager = BatchManager::new(LogWriterOptions::default(), fragment_uploader)
            .expect("batch manager");

        let err = batch_manager
            .read_json_file("gc/GARBAGE")
            .await
            .expect_err("preferred storage miss should not fall back to replicas");

        assert!(
            matches!(&err, Error::StorageError(storage_err) if matches!(&**storage_err, StorageError::NotFound { .. })),
            "expected preferred-storage NotFound, got {err:?}"
        );
    }
}
