use crate::segment_manager::SegmentManager;
use crate::sysdb::{GrpcSysDb, SysDb};
use crate::types::EmbeddingRecord;
use crate::Component;
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
        Arc<tokio::sync::RwLock<HashMap<String, async_channel::Sender<Box<EmbeddingRecord>>>>>,
    receiver: tokio::sync::mpsc::Receiver<Box<EmbeddingRecord>>,
    dispatchers: Vec<IngestDispatcherHandle>, // We need to inform the dispatcher when a new tenant is added
    sysdb: Box<dyn SysDb>,
    collection_to_tenant: HashMap<Uuid, String>, // Cache the collection to tenant mapping.
}

struct IngestSchedulerHandle {
    ingest_sender: tokio::sync::mpsc::Sender<Box<EmbeddingRecord>>,
}

impl IngestScheduler {
    fn new(
        reciever: tokio::sync::mpsc::Receiver<Box<EmbeddingRecord>>,
        dispatchers: Vec<IngestDispatcherHandle>,
        sysdb: Box<dyn SysDb>,
    ) -> Self {
        return IngestScheduler {
            tenant_channels: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            receiver: reciever,
            dispatchers: dispatchers,
            sysdb: sysdb,
            collection_to_tenant: HashMap::new(),
        };
    }

    async fn handle(&mut self, msg: Box<EmbeddingRecord>) {
        // Figure out which tenant this message belongs to
        println!("Handling message: {:?}", msg.id);
        let collection_id = msg.collection_id;
        let tenant_id = self.get_tenant_id(collection_id).await;
        println!("Handling message for tenant: {:?}", tenant_id);

        match tenant_id {
            Ok(tenant_id) => {
                // Send the message to the tenant channel
                self.send_to_tenant(&tenant_id, msg).await;
            }
            Err(e) => {}
        }
    }

    async fn get_tenant_id(&mut self, collection_id: Uuid) -> Result<String, &'static str> {
        // Check if we have the tenant cached
        let tenant_id = self.collection_to_tenant.get(&collection_id);
        match tenant_id {
            Some(tenant_id) => {
                return Ok(tenant_id.clone());
            }
            None => {
                // Get the tenant from sysdb
                let collections = self
                    .sysdb
                    .get_collections(Some(collection_id), None, None, None, None)
                    .await;
                match collections {
                    Ok(collections) => {
                        if collections.len() < 1 {
                            return Err("No collection found");
                        }
                        let collection = collections.get(0).unwrap();
                        let tenant_id = &collection.tenant;
                        // Cache the tenant
                        self.collection_to_tenant
                            .insert(collection_id, tenant_id.clone());
                        return Ok(tenant_id.clone());
                    }
                    Err(e) => {
                        return Err("Failed to get collection");
                    }
                }
            }
        }
    }

    async fn send_to_tenant(&self, tenant_id: &str, msg: Box<EmbeddingRecord>) {
        // Get the channel for this tenant
        let mut tenant_channels = self.tenant_channels.write().await;
        let channel = tenant_channels.get(tenant_id);
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
                        tenant_id: tenant_id.to_owned(),
                        queue: receiver.clone(),
                    };
                    dispatcher.sender.send(new_tenant_msg).await.unwrap();
                }
                // Send the message to the channel
                sender.send(msg).await.unwrap();
                // Add the channel to the tenant channels
                tenant_channels.insert(tenant_id.to_owned(), sender);
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
    fn new(dispatchers: Vec<IngestDispatcherHandle>, sysdb: Box<dyn SysDb>) -> Self {
        let (ingest_sender, ingest_receiver) = tokio::sync::mpsc::channel(10000);
        // TODO: use new not struct
        let scheduler = IngestScheduler {
            tenant_channels: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            receiver: ingest_receiver,
            dispatchers: dispatchers,
            sysdb: sysdb,
            collection_to_tenant: HashMap::new(),
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
    round_robin_queues: Vec<async_channel::Receiver<Box<EmbeddingRecord>>>,
    queue_reciever: tokio::sync::mpsc::Receiver<NewTenantMessage>,
    segment_manager: SegmentManager,
}

struct NewTenantMessage {
    tenant_id: String,
    queue: async_channel::Receiver<Box<EmbeddingRecord>>,
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
                    // TODO: IS AWAITING HERE OK? SINCE WE NEED ORDERING?
                    ingest_dispatcher.segment_manager.write_record(msg).await;
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
    use num_bigint::BigInt;

    use super::*;
    use crate::types::{EmbeddingRecord, Operation, ScalarEncoding};

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

    // #[tokio::test]
    // async fn test_ingest_scheduler() {
    //     let dispatch_count = 12;
    //     let segment_manager = SegmentManager::new();

    //     let sysdb = Box::new(sysdb::GrpcSysDb::new().await);

    //     let mut dispatchers = Vec::new();
    //     for _ in 0..dispatch_count {
    //         let dispatcher = IngestDispatcherHandle::new(segment_manager.clone());
    //         dispatchers.push(dispatcher);
    //     }
    //     let scheduler = IngestSchedulerHandle::new(dispatchers, sysdb);
    //     let mut records = Vec::new();
    //     let mut records2 = Vec::new();

    //     let mut rng: rand::prelude::ThreadRng = rand::thread_rng();

    //     // tenant1
    //     for i in 0..100000 {
    //         let mut data: Vec<f32> = Vec::new();
    //         for i in 0..960 {
    //             data.push(rng.gen());
    //         }

    //         let record = EmbeddingRecord {
    //             id: format!("id-{}", i),
    //             seq_id: BigInt::from(i),
    //             collection_id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
    //             embedding: Some(data),
    //             metadata: None,
    //             operation: Operation::Add,
    //             encoding: Some(ScalarEncoding::FLOAT32),
    //         };
    //         records.push(Box::new(record));
    //     }
    //     println!("Generated records");
    //     // // tenant2
    //     // for i in 0..5 {
    //     //     let record = EmbeddingRecord {
    //     //         id: format!("id-{}", i),
    //     //         collection_id: "test2".to_string(),
    //     //         vector: Some(chroma_proto::Vector {
    //     //             vector: vec![64, 160, 0, 0], // 5.0 in float32 little endian
    //     //             dimension: 1,
    //     //             encoding: chroma_proto::ScalarEncoding::Float32 as i32,
    //     //         }),
    //     //         metadata: None,
    //     //         operation: chroma_proto::Operation::Add as i32,
    //     //     };
    //     //     records2.push(Box::new(record));
    //     // }

    //     for record in records {
    //         scheduler.ingest_sender.send(record).await.unwrap();
    //     }
    //     for record in records2 {
    //         scheduler.ingest_sender.send(record).await.unwrap();
    //     }
    //     tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    // }

    use crate::assignment_policy::RendezvousHashingAssignmentPolicy;
    use crate::writer::Writer;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_end_to_end() {
        let dispatch_count = 1;
        let sysdb: Box<dyn SysDb> = Box::new(GrpcSysDb::new().await);
        let segment_manager = SegmentManager::new(sysdb.clone());

        let mut dispatchers = Vec::new();
        for _ in 0..dispatch_count {
            let dispatcher = IngestDispatcherHandle::new(segment_manager.clone());
            dispatchers.push(dispatcher);
        }
        let scheduler = IngestSchedulerHandle::new(dispatchers, sysdb);

        let (memberlist_sender, memberlist_receiver) = tokio::sync::broadcast::channel(10);
        let assignment_policy = Arc::new(Mutex::new(RendezvousHashingAssignmentPolicy::new(
            "default".to_string(),
            "default".to_string(),
        )));
        let mut writer = Writer::new(
            memberlist_receiver,
            assignment_policy,
            scheduler.ingest_sender,
        )
        .await;
        let memberlist = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        // tokio sleep
        writer.start();
        memberlist_sender.send(memberlist).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(45)).await;
        writer.stop();
        return;
    }
}
