use std::time::Duration;

use setsum::Setsum;
use tracing::Span;

use chroma_types::Cmek;

use crate::{
    Error, FragmentIdentifier, FragmentSeqNo, Garbage, GarbageCollectionOptions, LogPosition,
};

pub mod s3;

pub trait FragmentPointer: Clone + Send + 'static {
    fn identifier(&self) -> FragmentIdentifier;
    fn bootstrap(position: LogPosition) -> Self
    where
        Self: Sized;
}

impl FragmentPointer for (FragmentSeqNo, LogPosition) {
    fn identifier(&self) -> FragmentIdentifier {
        FragmentIdentifier::SeqNo(self.0)
    }

    fn bootstrap(position: LogPosition) -> Self {
        (FragmentSeqNo::BEGIN, position)
    }
}

#[async_trait::async_trait]
pub trait FragmentPublisherFactory {
    type FragmentPointer: FragmentPointer;
    type Publisher: FragmentPublisher<FragmentPointer = Self::FragmentPointer>;

    async fn make(&self) -> Result<Self::Publisher, Error>;
}

#[async_trait::async_trait]
pub trait FragmentPublisher: Send + Sync + 'static {
    type FragmentPointer: FragmentPointer;

    /// Enqueue work to be published.
    async fn push_work(
        &self,
        messages: Vec<Vec<u8>>,
        tx: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
        span: Span,
    );
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
    >;
    /// Finish the previous call to take_work.
    async fn finish_write(&self);

    /// Wait until take_work might have work.
    async fn wait_for_writable(&self);
    /// How long to sleep until take work might have work.
    fn until_next_time(&self) -> Duration;

    /// upload a parquet fragment
    async fn upload_parquet(
        &self,
        pointer: &Self::FragmentPointer,
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
    ) -> Result<(String, Setsum, usize), Error>;

    /// Start shutting down.  The shutdown is split for historical and unprincipled reasons.
    fn shutdown_prepare(&self);
    /// Finish shutting down.
    fn shutdown_finish(&self);
}

#[async_trait::async_trait]
pub trait ManifestPublisherFactory {
    type FragmentPointer: FragmentPointer;
    type Publisher: ManifestPublisher<Self::FragmentPointer>;

    async fn make(&self) -> Result<Self::Publisher, Error>;
}

#[async_trait::async_trait]
pub trait ManifestPublisher<FP: FragmentPointer>: Send + Sync + 'static {
    /// Recover the manifest so that it can do work.
    async fn recover(&mut self) -> Result<(), Error>;
    /// Assign a timestamp for the next fragment that's going to be published on this manifest.
    fn assign_timestamp(&self, record_count: usize) -> Option<FP>;
    /// Publish a fragment previously assigned a timestamp using assign_timestamp.
    async fn publish_fragment(
        &self,
        pointer: &FP,
        path: &str,
        messages_len: u64,
        num_bytes: u64,
        setsum: Setsum,
    ) -> Result<LogPosition, Error>;
    /// Check if the garbge will apply "cleanly", that is without violating invariants.
    async fn garbage_applies_cleanly(&self, garbage: &Garbage) -> Result<bool, Error>;
    /// Apply a garbage file to the manifest.
    async fn apply_garbage(&self, garbage: Garbage) -> Result<(), Error>;
    /// Compute the garbage assuming at least log position will be kept.
    async fn compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        first_to_keep: LogPosition,
    ) -> Result<Option<Garbage>, Error>;

    /// Shutdown the manifest manager.  Must be called between prepare and finish of
    /// FragmentPublisher shutdown.
    fn shutdown(&self);
}
