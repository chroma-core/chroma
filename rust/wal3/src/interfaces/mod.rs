use std::time::Duration;

use tracing::Span;

use crate::{Error, LogPosition, ManifestManager};

pub mod s3;

#[async_trait::async_trait]
pub trait FragmentPublisher {
    type FragmentPointer;

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
        manifest_manager: &ManifestManager,
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

    /// Start shutting down.  The shutdown is split for historical and unprincipled reasons.
    fn shutdown_prepare(&self);
    /// Finish shutting down.
    fn shutdown_finish(&self);
}
