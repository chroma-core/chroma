// use crate::chroma_proto;
// use crate::chroma_proto::{GetCollectionsRequest, GetCollectionsResponse, SubmitEmbeddingRecord};
// use crate::Component;
// use std::{collections::HashMap, sync::Arc};
// use tokio::{runtime::Builder, sync::RwLock};
// use uuid::Uuid;

// /*

// The ingest scheduler consumes a stream from the ingest log and schedules the records for
// each tenant into a given channel. Downstream components can then consume from the channel
// and respect tenant fairness by processing records from each tenant in a round-robin fashion.

// //TODO: it should send/rec Box<SubmitEmbeddingRecord> instead of SubmitEmbeddingRecord so
// // we avoid copying the record

// */
// struct IngestScheduler {
//     tenant_channels: Arc<RwLock<HashMap<Uuid, async_channel::Sender<Box<SubmitEmbeddingRecord>>>>>,
//     // Keep a list of channel of processors to send round robin queues to
//     processor_tenant_channel_updates: Vec<
//     running: bool,
//     // TODO: add cancellation token
// }

// struct IngestSchedulerHandle {
//     ingest_sender: tokio::sync::mpsc::Sender<Box<SubmitEmbeddingRecord>>,
//     subscription_sender: tokio::sync::mpsc::Sender<IngestTenantChannelMessage>,
// }

// // Used to send a channel to the processor for a given tenant
// struct IngestTenantChannelMessage {
//     uuid: Uuid,
//     receiver: async_channel::Receiver<Box<SubmitEmbeddingRecord>>,
// }

// impl IngestScheduler {
//     fn new(receiver: tokio::sync::mpsc::Receiver<Box<SubmitEmbeddingRecord>>) -> IngestScheduler {
//         return IngestScheduler {
//             tenant_channels: Arc::new(RwLock::new(HashMap::new())),
//             processor_tenant_channel_updates: Vec::new(),
//             running: false,
//         };
//     }

//     fn subscribe_to_tenant_channel_updates(
//         &mut self,
//         sender: tokio::sync::mpsc::Sender<IngestTenantChannelMessage>,
//     ) {
//         self.processor_tenant_channel_updates.push(sender);
//     }
// }

// impl Component for IngestScheduler {
//     fn start(&mut self) {
//         if self.running {
//             return;
//         }

//         self.running = true;

//         let mut tenant_channels_lock = self.tenant_channels.clone();
//         let mut receiver = self.receiver;

//         tokio::spawn(async move {
//             let mut collection_uuid_to_tenant_uuid = HashMap::<Uuid, Uuid>::new();
//             // TODO: don't unwrap and pass config
//             let mut sys_db_client =
//                 chroma_proto::sys_db_client::SysDbClient::connect("http://localhost:50051")
//                     .await
//                     .unwrap();

//             while let record = receiver.recv().await {
//                 match record {
//                     Some(record) => {
//                         // // TODO: don't unwrap
//                         let collection_uuid = Uuid::parse_str(&record.collection_id).unwrap();
//                         let tenant_uuid: &Uuid;
//                         if collection_uuid_to_tenant_uuid.contains_key(&collection_uuid) {
//                             tenant_uuid = collection_uuid_to_tenant_uuid
//                                 .get(&collection_uuid)
//                                 .unwrap();
//                         } else {
//                             // use sysdb to get the tenant id
//                             // TODO: move into interface and use a cache
//                             let sys_db_resp = sys_db_client
//                                 .get_collections(chroma_proto::GetCollectionsRequest {
//                                     id: Some(record.collection_id.clone()),
//                                     name: None,
//                                     topic: None,
//                                     tenant: "".to_string(), // TODO: can't be None
//                                     database: "".to_string(),
//                                 })
//                                 .await
//                                 .unwrap();

//                             let get_collections_resp = sys_db_resp.into_inner();
//                             let tenant_id = &get_collections_resp.collections[0].tenant;
//                             let parsed_tenant_uuid = Uuid::parse_str(tenant_id).unwrap();
//                             tenant_uuid = &parsed_tenant_uuid;
//                             collection_uuid_to_tenant_uuid.insert(collection_uuid, *tenant_uuid);
//                             println!("Got tenant uuid: {:?}", tenant_uuid);

//                             // let mut sender: Option<
//                             //     async_channel::Sender<Box<SubmitEmbeddingRecord>>,
//                             // > = None;
//                             // {
//                             //     let tenant_channels = tenant_channels_lock.read().await;
//                             //     // TODO: handle unwrap
//                             //     let res = tenant_channels.get(tenant_uuid);
//                             //     if !res.is_none() {
//                             //         sender = Some(res.unwrap().0.clone()); // TODO: can we remove the clone?
//                             //     }
//                             //     // TODO: handle error
//                             //     // If the tenant channel doesn't exist, allow the lock to be dropped, thus unlocking the read lock
//                             //     // then promote the lock to a write lock and create the channel
//                             // }
//                             // // Only take the write lock if we need to create the channel for a new tenant
//                             // if sender.is_none() {
//                             //     let mut tenant_channels = tenant_channels_lock.write().await;
//                             //     // TODO: parameterize channel size
//                             //     let (sender, receiver) = async_channel::bounded(10000);
//                             //     tenant_channels.insert(*tenant_uuid, (sender, receiver));
//                             // }
//                             // let sender_unwrapped = sender.unwrap(); //TODO: handle unwrap better
//                             // let res = sender_unwrapped.send(Box::new(record)).await; // TODO: This should come in as box and we should send a box everywhere, transferring ownership to the next component

//                             // if res.is_err() {
//                             //     // TODO: handle error
//                             //     println!("Error sending to tenant channel: {:?}", res);
//                             // }
//                         }
//                     }
//                     None => {
//                         println!("Got None")
//                     }
//                 }
//             }
//         });
//     }

//     fn stop(&mut self) {
//         // TODO: implement
//     }
// }

// /*

// An IngestProcessors bridges async and sync code. It spawns a number of threads and
// each thread has acccess to a channel for each tenant. The thread polls
// the channels in a round-robin fashion and processes the records.

// */
// struct IngestDispatch {
//     tenant_channels: Arc<
//         RwLock<
//             HashMap<
//                 Uuid,
//                 (
//                     async_channel::Sender<Box<SubmitEmbeddingRecord>>,
//                     async_channel::Receiver<Box<SubmitEmbeddingRecord>>,
//                 ),
//             >,
//         >,
//     >,
//     num_threads: usize,
//     running: bool,
// }

// impl IngestDispatch {
//     fn new(
//         tenant_channels: Arc<
//             RwLock<
//                 HashMap<
//                     Uuid,
//                     (
//                         async_channel::Sender<Box<SubmitEmbeddingRecord>>,
//                         async_channel::Receiver<Box<SubmitEmbeddingRecord>>,
//                     ),
//                 >,
//             >,
//         >,
//         num_threads: usize,
//     ) -> IngestDispatch {
//         return IngestDispatch {
//             tenant_channels: tenant_channels,
//             num_threads: num_threads,
//             running: false,
//         };
//     }
// }

// impl Component for IngestDispatch {
//     fn start(&mut self) {
//         if self.running {
//             return;
//         }

//         self.running = true;

//         let tenant_channels = self.tenant_channels.clone();
//         let num_threads = self.num_threads;
//         let thread = std::thread::spawn(move || {
//             // a thread pool for cpu bound tasks
//             // inspired by: https://www.influxdata.com/blog/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/
//             // and https://www.youtube.com/watch?v=UcW93axVcek
//             let cpu_bound_tokio_runtime = Builder::new_multi_thread()
//                 .worker_threads(num_threads)
//                 .enable_all()
//                 .build()
//                 .unwrap();

//             cpu_bound_tokio_runtime.block_on(async move {
//                 let mut tenant_channels = tenant_channels.read().await;
//                 let mut tenant_channels = tenant_channels.iter().cycle();
//                 loop {
//                     let (tenant_uuid, (sender, receiver)) = tenant_channels.next().unwrap();
//                     let record = receiver.try_recv();
//                     if record.is_ok() {
//                         println!("Got record for tenant: {:?}", tenant_uuid);
//                         println!("On thread: {:?}", std::thread::current().id());
//                     }
//                     // todo: handle error
//                     // sleep to avoid spinning
//                     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
//                 }
//             });
//         });
//     }

//     fn stop(&mut self) {
//         // TODO: implement
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::assignment_policy::RendezvousHashingAssignmentPolicy;
//     use crate::writer::Writer;
//     use tokio::sync::Mutex;

//     #[tokio::test]
//     async fn it_can_work() {
//         // create a writer
//         let (memberlist_sender, memberlist_receiver) = tokio::sync::broadcast::channel(10);
//         let assignment_policy = Arc::new(Mutex::new(RendezvousHashingAssignmentPolicy::new(
//             "default".to_string(),
//             "default".to_string(),
//         )));
//         let (msg_sender, msg_receiver) = tokio::sync::mpsc::channel(10000);
//         let mut writer = Writer::new(memberlist_receiver, assignment_policy, msg_sender).await;
//         let memberlist = vec!["a".to_string(), "b".to_string(), "c".to_string()];
//         // tokio sleep
//         writer.start();
//         memberlist_sender.send(memberlist).unwrap();
//         tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
//         writer.stop();

//         let scheduler = IngestScheduler::new(msg_receiver);
//     }
// }
