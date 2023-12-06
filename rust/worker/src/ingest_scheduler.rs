use crate::chroma_proto;
use crate::chroma_proto::{GetCollectionsRequest, GetCollectionsResponse, SubmitEmbeddingRecord};
use crate::Component;
use futures::{future, Future};
use std::{collections::HashMap, sync::Arc};
use tokio::runtime::Builder;
use uuid::Uuid;

/*

The ingest scheduler consumes a stream from the ingest log and schedules the records for
each tenant into a given channel. Downstream components can then consume from the channel
and respect tenant fairness by processing records from each tenant in a round-robin fashion.

//TODO: it should send/rec Box<SubmitEmbeddingRecord> instead of SubmitEmbeddingRecord so
// we avoid copying the record

*/
struct IngestScheduler {
    tenant_channels:
        Arc<tokio::sync::RwLock<HashMap<Uuid, async_channel::Sender<Box<SubmitEmbeddingRecord>>>>>,
    receiver: tokio::sync::mpsc::Receiver<Box<SubmitEmbeddingRecord>>,
    dispatchers: Vec<IngestDispatcherHandle>, // We need to inform the dispatcher when a new tenant is added
}

struct IngestSchedulerHandle {
    ingest_sender: tokio::sync::mpsc::Sender<Box<SubmitEmbeddingRecord>>,
}

impl IngestScheduler {
    fn new(
        reciever: tokio::sync::mpsc::Receiver<Box<SubmitEmbeddingRecord>>,
        dispatchers: Vec<IngestDispatcherHandle>,
    ) -> Self {
        return IngestScheduler {
            tenant_channels: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            receiver: reciever,
            dispatchers: dispatchers,
        };
    }

    async fn handle(&self, msg: Box<SubmitEmbeddingRecord>) {
        //TODO replace with sysdb lookup
        let tenant_id;
        if msg.collection_id == "test" {
            tenant_id = "550e8400-e29b-41d4-a716-446655440000";
        } else {
            tenant_id = "550e8400-e29b-41d4-a716-446655440001";
        }
        // Figure out which tenant this message belongs to
        let tenant_uuid = Uuid::parse_str(tenant_id);
        match tenant_uuid {
            Ok(tenant_uuid) => {
                // Send the message to the tenant channel
                self.send_to_tenant(tenant_uuid, msg).await;
            }
            Err(e) => {}
        }
    }

    async fn send_to_tenant(&self, tenant_id: Uuid, msg: Box<SubmitEmbeddingRecord>) {
        // Get the channel for this tenant
        let mut tenant_channels = self.tenant_channels.write().await;
        let channel = tenant_channels.get(&tenant_id);
        match channel {
            Some(_) => {
                // Send the message to the channel
                channel.unwrap().send(msg).await.unwrap();
            }
            None => {
                // Create a new channel for this tenant
                let (sender, receiver) = async_channel::bounded(10000);
                // Inform the dispatcher that a new tenant has been added
                for dispatcher in &self.dispatchers {
                    let new_tenant_msg = NewTenantMessage {
                        tenant_id: tenant_id,
                        queue: receiver.clone(),
                    };
                    dispatcher.sender.send(new_tenant_msg).await.unwrap();
                }
                // Send the message to the channel
                sender.send(msg).await.unwrap();
                // Add the channel to the tenant channels
                tenant_channels.insert(tenant_id, sender);
            }
        }
    }
}

async fn run_ingest_scheduler(mut ingest_scheduler: IngestScheduler) {
    while let Some(msg) = ingest_scheduler.receiver.recv().await {
        ingest_scheduler.handle(msg).await;
    }
}

impl IngestSchedulerHandle {
    fn new(dispatchers: Vec<IngestDispatcherHandle>) -> Self {
        let (ingest_sender, ingest_receiver) = tokio::sync::mpsc::channel(10000);
        let scheduler = IngestScheduler {
            tenant_channels: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            receiver: ingest_receiver,
            dispatchers: dispatchers,
        };
        // Spawns on the main tokio runtime
        tokio::spawn(async move {
            run_ingest_scheduler(scheduler).await;
        });
        return IngestSchedulerHandle {
            ingest_sender: ingest_sender,
        };
    }
}

// =================================================================================================

// Implements a round-robin dispatching policy for the ingest scheduler
struct IngestDispatcher {
    // We use async_channel because it supports exclusive reads
    round_robin_queues: Vec<async_channel::Receiver<Box<SubmitEmbeddingRecord>>>,
    queue_reciever: tokio::sync::mpsc::Receiver<NewTenantMessage>,
}

struct NewTenantMessage {
    tenant_id: Uuid,
    queue: async_channel::Receiver<Box<SubmitEmbeddingRecord>>,
}

struct IngestDispatcherHandle {
    sender: tokio::sync::mpsc::Sender<NewTenantMessage>,
}

impl IngestDispatcher {
    fn new(queue_reciever: tokio::sync::mpsc::Receiver<NewTenantMessage>) -> Self {
        return IngestDispatcher {
            round_robin_queues: Vec::new(),
            queue_reciever: queue_reciever,
        };
    }

    async fn handle(&mut self, msg: NewTenantMessage) {
        self.round_robin_queues.push(msg.queue);
    }
}

async fn run_ingest_dispatcher(mut ingest_dispatcher: IngestDispatcher) {
    loop {
        let new_queue = ingest_dispatcher.queue_reciever.try_recv();
        match new_queue {
            Ok(new_queue) => {
                println!("got new queue for tenant: {:?}", new_queue.tenant_id);
                ingest_dispatcher.handle(new_queue).await;
            }
            Err(e) => match e {
                tokio::sync::mpsc::error::TryRecvError::Empty => {
                    // print!("error: {:?}", e);
                    // No new queue available
                }
                tokio::sync::mpsc::error::TryRecvError::Disconnected => {
                    // print!("error: {:?}", e);
                }
            },
        }

        // Round robin poll the queues
        // This spinning is not the best way to do this
        // We should instead signal the dispatcher that a queue is ready to be polled
        // However it is good enough for now
        for i in 0..ingest_dispatcher.round_robin_queues.len() {
            let resp = ingest_dispatcher.round_robin_queues[i].try_recv();
            match resp {
                Ok(msg) => {
                    // Process the message here
                    println!(
                        "processing message: {:?} on thread: {:?}",
                        msg,
                        std::thread::current().id()
                    );
                    // Simulate CPU work by sleeping
                    std::thread::sleep(std::time::Duration::from_micros(500));
                }
                Err(e) => {
                    // TODO: check error type
                    match e {
                        async_channel::TryRecvError::Empty => {
                            // No message available
                        }
                        async_channel::TryRecvError::Closed => {
                            // TODO: deal with this case correctly
                        }
                    }
                }
            }
        }
        tokio::task::yield_now().await;
    }
}

impl IngestDispatcherHandle {
    fn new() -> Self {
        let (queue_sender, queue_receiver) = tokio::sync::mpsc::channel(10000);
        let dispatcher = IngestDispatcher::new(queue_receiver);
        // Spawn this on a dedicated thread
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        std::thread::spawn(move || {
            rt.block_on(async move {
                run_ingest_dispatcher(dispatcher).await;
            });
        });
        return IngestDispatcherHandle {
            sender: queue_sender,
        };
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::chroma_proto::SubmitEmbeddingRecord;

    #[tokio::test]
    async fn test_ingest_scheduler() {
        let dispatch_count = 12;
        let mut dispatchers = Vec::new();
        for _ in 0..dispatch_count {
            let dispatcher = IngestDispatcherHandle::new();
            dispatchers.push(dispatcher);
        }
        let scheduler = IngestSchedulerHandle::new(dispatchers);
        let mut records = Vec::new();
        let mut records2 = Vec::new();

        // tenant1
        for i in 0..10000 {
            let record = SubmitEmbeddingRecord {
                id: format!("id-{}", i),
                collection_id: "test".to_string(),
                vector: None,
                metadata: None,
                operation: chroma_proto::Operation::Upsert as i32,
            };
            records.push(Box::new(record));
        }
        // tenant2
        for i in 0..1 {
            let record = SubmitEmbeddingRecord {
                id: format!("id-{}", i),
                collection_id: "test2".to_string(),
                vector: None,
                metadata: None,
                operation: chroma_proto::Operation::Upsert as i32,
            };
            records2.push(Box::new(record));
        }

        for record in records {
            scheduler.ingest_sender.send(record).await.unwrap();
        }
        for record in records2 {
            scheduler.ingest_sender.send(record).await.unwrap();
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
