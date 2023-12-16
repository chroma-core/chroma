use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{Arc, RwLock},
};

use crate::{
    assignment::{
        self,
        assignment_policy::{self, AssignmentPolicy},
    },
    chroma_proto,
    config::{Configurable, WorkerConfig},
    errors::{ChromaError, ErrorCodes},
    memberlist::{CustomResourceMemberlistProvider, Memberlist},
    sysdb::sysdb::{GrpcSysDb, SysDb},
    system::{Component, ComponentContext, ComponentHandle, Handler, StreamHandler},
    types::{EmbeddingRecord, EmbeddingRecordConversionError, SeqId},
};

use pulsar::{
    consumer::topic, Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor,
};
use thiserror::Error;

use super::message_id::PulsarMessageIdWrapper;

/// An ingest component is responsible for ingesting data into the system from the log
/// stream.
/// # Notes
/// The only current implementation of the ingest is the Pulsar ingest.
pub(crate) struct Ingest {
    assignment_policy: RwLock<Box<dyn AssignmentPolicy + Sync + Send>>,
    assigned_topics: RwLock<Vec<String>>,
    topic_to_handle: RwLock<HashMap<String, ComponentHandle<PulsarIngestTopic>>>,
    queue_size: usize,
    my_ip: String,
    pulsar_tenant: String,
    pulsar_namespace: String,
    pulsar: Pulsar<TokioExecutor>,
    sysdb: Box<dyn SysDb>,
}

impl Component for Ingest {
    fn queue_size(&self) -> usize {
        return self.queue_size;
    }
}

impl Debug for Ingest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ingest")
            .field("queue_size", &self.queue_size)
            .finish()
    }
}

#[derive(Error, Debug)]
pub(crate) enum IngestConfigurationError {
    #[error(transparent)]
    PulsarError(#[from] pulsar::Error),
}

impl ChromaError for IngestConfigurationError {
    fn code(&self) -> ErrorCodes {
        match self {
            IngestConfigurationError::PulsarError(_e) => ErrorCodes::Internal,
        }
    }
}

// TODO: Nest the ingest assignment policy inside the ingest component config so its
// specific to the ingest component and can be used here
#[async_trait]
impl Configurable for Ingest {
    async fn try_from_config(worker_config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
        let assignment_policy = assignment_policy::RendezvousHashingAssignmentPolicy::new(
            worker_config.pulsar_tenant.clone(),
            worker_config.pulsar_namespace.clone(),
        );

        let pulsar = match Pulsar::builder(worker_config.pulsar_url.clone(), TokioExecutor)
            .build()
            .await
        {
            Ok(pulsar) => pulsar,
            Err(e) => {
                return Err(Box::new(IngestConfigurationError::PulsarError(e)));
            }
        };

        // TODO: Sysdb should have a dynamic resolution in sysdb
        let sysdb = GrpcSysDb::try_from_config(worker_config).await;
        let sysdb = match sysdb {
            Ok(sysdb) => sysdb,
            Err(err) => {
                return Err(err);
            }
        };

        let ingest = Ingest {
            assignment_policy: RwLock::new(Box::new(assignment_policy)),
            assigned_topics: RwLock::new(vec![]),
            topic_to_handle: RwLock::new(HashMap::new()),
            queue_size: worker_config.ingest.queue_size,
            my_ip: worker_config.my_ip.clone(),
            pulsar: pulsar,
            pulsar_tenant: worker_config.pulsar_tenant.clone(),
            pulsar_namespace: worker_config.pulsar_namespace.clone(),
            sysdb: Box::new(sysdb),
        };
        Ok(ingest)
    }
}

impl Ingest {
    fn get_topics(&self) -> Vec<String> {
        // This mirrors the current python and go code, which assumes a fixed set of topics
        let mut topics = Vec::with_capacity(16);
        for i in 0..16 {
            let topic = format!(
                "persistent://{}/{}/chroma_log_{}",
                self.pulsar_tenant, self.pulsar_namespace, i
            );
            topics.push(topic);
        }
        return topics;
    }
}

#[async_trait]
impl Handler<Memberlist> for Ingest {
    async fn handle(&mut self, msg: Memberlist, ctx: &ComponentContext<Self>) {
        let mut new_assignments = HashSet::new();
        let candidate_topics: Vec<String> = self.get_topics();
        // Scope for assigner write lock to be released so we don't hold it over await
        {
            let mut assigner = match self.assignment_policy.write() {
                Ok(assigner) => assigner,
                Err(err) => {
                    println!("Failed to read assignment policy: {:?}", err);
                    return;
                }
            };

            // Use the assignment policy to assign topics to this worker
            assigner.set_members(msg);
            for topic in candidate_topics.iter() {
                let assignment = assigner.assign(topic);
                let assignment = match assignment {
                    Ok(assignment) => assignment,
                    Err(err) => {
                        // TODO: Log error
                        continue;
                    }
                };
                if assignment == self.my_ip {
                    new_assignments.insert(topic);
                }
            }
        }

        // Compute the topics we need to add/remove
        let mut to_remove = Vec::new();
        let mut to_add = Vec::new();

        // Scope for assigned topics read lock to be released so we don't hold it over await
        {
            let assigned_topics_handle = self.assigned_topics.read();
            match assigned_topics_handle {
                Ok(assigned_topics) => {
                    // Compute the diff between the current assignments and the new assignments
                    for topic in assigned_topics.iter() {
                        if !new_assignments.contains(topic) {
                            to_remove.push(topic.to_string());
                        }
                    }
                    for topic in new_assignments.iter() {
                        if !assigned_topics.contains(*topic) {
                            to_add.push(topic.to_string());
                        }
                    }
                }
                Err(err) => {
                    // TODO: Log error and handle lock poisoning
                }
            }
        }

        // Unsubscribe from topics we no longer need to listen to
        for topic in to_remove.iter() {
            match self.topic_to_handle.write() {
                Ok(mut topic_to_handle) => {
                    let handle = topic_to_handle.remove(topic);
                    match handle {
                        Some(mut handle) => {
                            handle.stop();
                        }
                        None => {
                            // TODO: This should log an error
                            println!("No handle found for topic: {}", topic);
                        }
                    }
                }
                Err(err) => {
                    // TODO: Log an error and handle lock poisoning
                }
            }
        }

        // Subscribe to new topics
        for topic in to_add.iter() {
            println!("Adding topic: {}", topic);
            // Do the subscription and register the stream to this ingest component
            let consumer: Consumer<chroma_proto::SubmitEmbeddingRecord, TokioExecutor> = self
                .pulsar
                .consumer()
                .with_topic(topic.to_string())
                .with_subscription_type(SubType::Exclusive)
                .build()
                .await
                .unwrap();

            let ingest_topic_component = PulsarIngestTopic::new(consumer, self.sysdb.clone());

            let handle = ctx.system.clone().start_component(ingest_topic_component);

            // Bookkeep the handle so we can shut the stream down later
            match self.topic_to_handle.write() {
                Ok(mut topic_to_handle) => {
                    topic_to_handle.insert(topic.to_string(), handle);
                }
                Err(err) => {
                    // TODO: log error and handle lock poisoning
                    println!("Failed to write topic to handle: {:?}", err);
                }
            }
        }
    }
}

impl DeserializeMessage for chroma_proto::SubmitEmbeddingRecord {
    type Output = Self;

    fn deserialize_message(payload: &Payload) -> chroma_proto::SubmitEmbeddingRecord {
        // Its a bit strange to unwrap here, but the pulsar api doesn't give us a way to
        // return an error, so we have to panic if we can't decode the message
        // also we are forced to clone since the api doesn't give us a way to borrow the bytes
        // TODO: can we not clone?
        // TODO: I think just typing this to Result<> would allow errors to propagate
        let record =
            chroma_proto::SubmitEmbeddingRecord::decode(Bytes::from(payload.data.clone())).unwrap();
        return record;
    }
}

struct PulsarIngestTopic {
    consumer: RwLock<Option<Consumer<chroma_proto::SubmitEmbeddingRecord, TokioExecutor>>>,
    sysdb: Box<dyn SysDb>,
}

impl Debug for PulsarIngestTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarIngestTopic").finish()
    }
}

impl PulsarIngestTopic {
    fn new(
        consumer: Consumer<chroma_proto::SubmitEmbeddingRecord, TokioExecutor>,
        sysdb: Box<dyn SysDb>,
    ) -> Self {
        PulsarIngestTopic {
            consumer: RwLock::new(Some(consumer)),
            sysdb: sysdb,
        }
    }
}

impl Component for PulsarIngestTopic {
    fn queue_size(&self) -> usize {
        1000
    }

    fn on_start(&self, ctx: &ComponentContext<Self>) -> () {
        let stream = match self.consumer.write() {
            Ok(mut consumer_handle) => consumer_handle.take(),
            Err(err) => None,
        };
        let stream = match stream {
            Some(stream) => stream,
            None => {
                return;
            }
        };
        let stream = stream.then(|result| async {
            match result {
                Ok(msg) => {
                    // Convert the Pulsar Message to an EmbeddingRecord
                    let proto_embedding_record = msg.deserialize();
                    let id = msg.message_id;
                    let seq_id: SeqId = PulsarMessageIdWrapper(id).into();
                    let embedding_record: Result<EmbeddingRecord, EmbeddingRecordConversionError> =
                        (proto_embedding_record, seq_id).try_into();
                    match embedding_record {
                        Ok(embedding_record) => {
                            return Some(Arc::new(embedding_record));
                        }
                        Err(err) => {
                            // TODO: Handle and log
                        }
                    }
                    None
                }
                Err(err) => {
                    // TODO: Log an error
                    // Put this on a dead letter queue, this concept does not exist in our
                    // system yet
                    None
                }
            }
        });
        self.register_stream(stream, ctx);
    }
}

#[async_trait]
impl Handler<Option<Arc<EmbeddingRecord>>> for PulsarIngestTopic {
    async fn handle(
        &mut self,
        message: Option<Arc<EmbeddingRecord>>,
        _ctx: &ComponentContext<PulsarIngestTopic>,
    ) -> () {
        // Use the sysdb to tenant id for the embedding record
        let embedding_record = match message {
            Some(embedding_record) => embedding_record,
            None => {
                return;
            }
        };
        let coll = self
            .sysdb
            .get_collections(Some(embedding_record.collection_id), None, None, None, None)
            .await;
    }
}

#[async_trait]
impl StreamHandler<Option<Arc<EmbeddingRecord>>> for PulsarIngestTopic {}
