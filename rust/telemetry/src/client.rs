use crate::events::ProductTelemetryEvent;
use async_trait::async_trait;
use chroma_system::ReceiverForMessage;
use tokio::sync::OnceCell;

pub static EVENT_SENDER: OnceCell<
    Box<dyn ReceiverForMessage<Box<dyn ProductTelemetryEvent + Send + Sync>>>,
> = OnceCell::const_new();

#[async_trait]
pub trait TelemetryClient {
    async fn aggregate(&mut self, event: Box<dyn ProductTelemetryEvent + Send + Sync>);
    async fn flush(&mut self);
    async fn batch_and_flush(&mut self);
}
