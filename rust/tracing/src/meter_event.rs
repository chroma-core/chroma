use std::sync::OnceLock;

use tokio::{
    runtime::Handle,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tonic::async_trait;

#[derive(Clone, Debug)]
pub enum IoKind {
    Read {
        collection_record: u32,
        collection_dim: u32,
        where_complexity: u32,
        vector_complexity: u32,
    },
    Write {
        log_bytes: u64,
    },
}

#[derive(Clone, Debug)]
pub enum MeterEvent {
    Collection {
        tenant_id: String,
        database_name: String,
        io: IoKind,
    },
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
impl MeterEventHandler for () {}

pub fn init_meter_event_handler(mut handler: impl MeterEventHandler + Send + Sync + 'static) {
    let (tx, rx) = unbounded_channel();
    let runtime_handle = Handle::current();
    runtime_handle.spawn(async move { handler.listen(rx).await });
    if METER_EVENT_SENDER.set(tx).is_err() {
        tracing::error!("Meter event handler is already initialized")
    }
}
