use crate::chroma_proto::{GetCollectionsRequest, GetCollectionsResponse, SubmitEmbeddingRecord};
use crate::segment_manager::SegmentManager;
use crate::Component;
use crate::{chroma_proto, segment_manager};
use futures::{future, Future};
use rand::Rng;
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
    segment_manager: SegmentManager,
}

struct NewTenantMessage {
    tenant_id: Uuid,
    queue: async_channel::Receiver<Box<SubmitEmbeddingRecord>>,
}

struct IngestDispatcherHandle {
    sender: tokio::sync::mpsc::Sender<NewTenantMessage>,
}

impl IngestDispatcher {
    fn new(
        queue_reciever: tokio::sync::mpsc::Receiver<NewTenantMessage>,
        segment_manager: SegmentManager,
    ) -> Self {
        return IngestDispatcher {
            round_robin_queues: Vec::new(),
            queue_reciever: queue_reciever,
            segment_manager: segment_manager,
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
        // We should instead signal the dispatcher that a queue is ready to be polled by using Notify
        // We can do this in a follow up
        for i in 0..ingest_dispatcher.round_robin_queues.len() {
            let resp = ingest_dispatcher.round_robin_queues[i].try_recv();
            match resp {
                Ok(msg) => {
                    // Process the message here
                    // println!(
                    //     "processing message: {:?} on thread: {:?}",
                    //     msg,
                    //     std::thread::current().id()
                    // );
                    ingest_dispatcher.segment_manager.write_record(msg);
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
        // sleep thread to avoid spinning
        std::thread::yield_now();
        std::thread::sleep(std::time::Duration::from_nanos(1000));
    }
}

impl IngestDispatcherHandle {
    fn new(segment_manager: SegmentManager) -> Self {
        let (queue_sender, queue_receiver) = tokio::sync::mpsc::channel(10000);
        let dispatcher = IngestDispatcher::new(queue_receiver, segment_manager);
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

    // #[tokio::test]
    // async fn test_ingest_scheduler() {
    //     let dispatch_count = 12;
    //     let mut dispatchers = Vec::new();
    //     for _ in 0..dispatch_count {
    //         let dispatcher = IngestDispatcherHandle::new();
    //         dispatchers.push(dispatcher);
    //     }
    //     let scheduler = IngestSchedulerHandle::new(dispatchers);
    //     let mut records = Vec::new();
    //     let mut records2 = Vec::new();

    //     // tenant1
    //     for i in 0..10000 {
    //         let record = SubmitEmbeddingRecord {
    //             id: format!("id-{}", i),
    //             collection_id: "test".to_string(),
    //             vector: None,
    //             metadata: None,
    //             operation: chroma_proto::Operation::Upsert as i32,
    //         };
    //         records.push(Box::new(record));
    //     }
    //     // tenant2
    //     for i in 0..1 {
    //         let record = SubmitEmbeddingRecord {
    //             id: format!("id-{}", i),
    //             collection_id: "test2".to_string(),
    //             vector: None,
    //             metadata: None,
    //             operation: chroma_proto::Operation::Upsert as i32,
    //         };
    //         records2.push(Box::new(record));
    //     }

    //     for record in records {
    //         scheduler.ingest_sender.send(record).await.unwrap();
    //     }
    //     for record in records2 {
    //         scheduler.ingest_sender.send(record).await.unwrap();
    //     }
    //     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // }

    #[tokio::test]
    async fn test_ingest_scheduler() {
        let dispatch_count = 12;
        let segment_manager = SegmentManager::new();

        let mut dispatchers = Vec::new();
        for _ in 0..dispatch_count {
            let dispatcher = IngestDispatcherHandle::new(segment_manager.clone());
            dispatchers.push(dispatcher);
        }
        let scheduler = IngestSchedulerHandle::new(dispatchers);
        let mut records = Vec::new();
        let mut records2 = Vec::new();

        let mut rng: rand::prelude::ThreadRng = rand::thread_rng();

        // tenant1
        for i in 0..100000 {
            let mut data: Vec<f32> = Vec::new();
            for i in 0..960 {
                data.push(rng.gen());
            }
            // Convert to bytes
            let mut bytes: Vec<u8> = Vec::new();
            for f in data {
                let sub_bytes = f.to_le_bytes();
                for b in sub_bytes {
                    bytes.push(b);
                }
            }
            let record = SubmitEmbeddingRecord {
                id: format!("id-{}", i),
                collection_id: "test".to_string(),
                vector: Some(chroma_proto::Vector {
                    vector: bytes,
                    dimension: 1,
                    encoding: chroma_proto::ScalarEncoding::Float32 as i32,
                }),
                metadata: None,
                operation: chroma_proto::Operation::Add as i32,
            };
            records.push(Box::new(record));
        }
        println!("Generated records");
        // tenant2
        for i in 0..5 {
            let record = SubmitEmbeddingRecord {
                id: format!("id-{}", i),
                collection_id: "test2".to_string(),
                vector: Some(chroma_proto::Vector {
                    vector: vec![64, 160, 0, 0], // 5.0 in float32 little endian
                    dimension: 1,
                    encoding: chroma_proto::ScalarEncoding::Float32 as i32,
                }),
                metadata: None,
                operation: chroma_proto::Operation::Add as i32,
            };
            records2.push(Box::new(record));
        }

        for record in records {
            scheduler.ingest_sender.send(record).await.unwrap();
        }
        for record in records2 {
            scheduler.ingest_sender.send(record).await.unwrap();
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}
