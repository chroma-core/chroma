use std::sync::OnceLock;

use tokio::{
    runtime::Handle,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tonic::async_trait;

#[derive(Clone, Debug)]
pub enum MeterEvent {
    Heartbeat(u128),
}

pub static METER_EVENT_SENDER: OnceLock<UnboundedSender<MeterEvent>> = OnceLock::new();

impl MeterEvent {
    pub async fn submit(self) {
        if let Some(handler) = METER_EVENT_SENDER.get() {
            if let Err(err) = handler.send(self) {
                tracing::error!("Unable to send meter event: {err}")
            }
        } else {
            tracing::error!("Meter event handler is unintialized")
        }
    }
}

#[async_trait]
pub trait MeterEventHandler {
    async fn handle(&mut self, _event: MeterEvent) {}
    async fn listen(&mut self, mut rx: UnboundedReceiver<MeterEvent>) {
        while let Some(event) = rx.recv().await {
            self.handle(event).await
        }
    }
}

#[async_trait]
impl MeterEventHandler for () {
    async fn handle(&mut self, event: MeterEvent) {
        println!("Metering event: {event:?}")
    }
}

pub fn init_meter_event_handler(mut handler: impl MeterEventHandler + Send + Sync + 'static) {
    let (tx, rx) = unbounded_channel();
    let runtime_handle = Handle::current();
    runtime_handle.spawn(async move { handler.listen(rx).await });
    if METER_EVENT_SENDER.set(tx).is_err() {
        tracing::error!("Meter event handler is already initialized")
    }
}
