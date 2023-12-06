use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::chroma_proto::SubmitEmbeddingRecord;
use crate::rendezvous_hash;
use crate::{
    assignment_policy::{AssignmentPolicy, RendezvousHashingAssignmentPolicy},
    memberlist_provider::Memberlist,
    Component,
};
use bytes::Bytes;
use futures::TryStreamExt;
use prost::Message;
use pulsar::{Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor};
use tokio::sync::Mutex;
use tokio::{select, sync::broadcast::Receiver};
use tokio_util::sync::CancellationToken;

// TODO: rename to IngestLog

impl DeserializeMessage for SubmitEmbeddingRecord {
    type Output = Self;

    fn deserialize_message(payload: &Payload) -> SubmitEmbeddingRecord {
        // Its a bit strange to unwrap here, but the pulsar api doesn't give us a way to
        // return an error, so we have to panic if we can't decode the message
        // also we are forced to clone since the api doesn't give us a way to borrow the bytes
        //TODO: can we not clone?
        let record = SubmitEmbeddingRecord::decode(Bytes::from(payload.data.clone())).unwrap();
        return record;
    }
}

pub struct Writer {
    memberlist_channel: Receiver<Vec<String>>,
    assignment_policy: Arc<Mutex<dyn AssignmentPolicy>>,
    cancellation_token: CancellationToken, // TODO: cancellation token needs to be refreshed when we strt/stop
    pulsar: Pulsar<TokioExecutor>,
    sender: tokio::sync::mpsc::Sender<Box<SubmitEmbeddingRecord>>,
    running: bool,
}

impl Writer {
    pub async fn new(
        memberlist_channel: Receiver<Memberlist>,
        assignment_policy: Arc<Mutex<dyn AssignmentPolicy>>,
        sender: tokio::sync::mpsc::Sender<Box<SubmitEmbeddingRecord>>,
    ) -> Writer {
        /// TODO: cleanup and configure
        let pulsar: Pulsar<TokioExecutor> =
            Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
                .build()
                .await
                .unwrap();

        return Writer {
            memberlist_channel: memberlist_channel,
            assignment_policy: assignment_policy,
            cancellation_token: CancellationToken::new(),
            pulsar: pulsar, // should we box this?
            sender: sender,
            running: false,
        };
    }
}

impl Component for Writer {
    fn start(&mut self) {
        if self.running {
            return;
        }

        self.running = true;

        // Spawn a task that manages subscriptions to topics
        let mut memberlist_channel = self.memberlist_channel.resubscribe(); // cloning receiver is cheap
        let cancellation_token = self.cancellation_token.clone();
        let assignment_policy_lock = self.assignment_policy.clone();
        let pulsar = self.pulsar.clone();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            print!("starting writer");
            // Each subscription will spawn a task that consumes messages from the topic
            // This master task will manage the subscriptions and spawn the consumer tasks
            // When the memberlist changes, this task will update the subscriptions
            // by shutting them down and potentially starting new ones
            // TODO: wait on proper shutdown
            let mut topic_to_cancel_token: HashMap<String, CancellationToken> = HashMap::new();
            let current_assigned_topics: HashSet<String> = HashSet::new();
            let hasher = rendezvous_hash::Murmur3Hasher {}; // TODO: should this be config?
            let mut new_assignments = HashSet::new();
            loop {
                select! {
                        _ = cancellation_token.cancelled() => {
                            // cancel all subscriptions
                            for (_, cancellation_token) in topic_to_cancel_token {
                                cancellation_token.cancel();
                            }
                            break;
                        }
                        memberlist = memberlist_channel.recv() => {
                            // TODO: properly unwrap etc
                            if memberlist.is_err() {
                                break;
                            }
                            let memberlist = memberlist.unwrap();
                            println!("got memberlist: {:?}", memberlist);
                            let target_topics;
                            {
                                let assignment_policy = assignment_policy_lock.lock().await;
                                target_topics = assignment_policy.get_topics();
                            }
                            new_assignments.clear();
                            for topic in target_topics {
                                let assigned = rendezvous_hash::assign(&topic, &memberlist, &hasher);
                                if assigned.is_err() {
                                    // TODO: log
                                    continue;
                                }
                                // TODO: check if assigned == me
                                let assigned = assigned.unwrap();
                                new_assignments.insert(topic);
                            }
                            let new_new_assignments = new_assignments.difference(&current_assigned_topics);
                            let net_old_assignments = current_assigned_topics.difference(&new_assignments);
                            for topic in new_new_assignments {
                                println!("subscribing to {}", topic);
                                let mut consumer: Consumer<SubmitEmbeddingRecord, TokioExecutor> = pulsar
                                    .consumer()
                                    .with_topic(topic)
                                    .with_consumer_name("consumer-name") // todo: config
                                    .with_subscription_type(SubType::Exclusive)
                                    .with_subscription("subscription-name") // todo: config
                                    .build()
                                    .await
                                    .unwrap();
                                let per_topic_cancel_token = CancellationToken::new();
                                topic_to_cancel_token.insert(topic.to_string(), per_topic_cancel_token.clone());
                                let sender = sender.clone();
                                tokio::spawn(async move {
                                    loop {
                                        select! {
                                            _ = per_topic_cancel_token.cancelled() => {
                                                break;
                                            }
                                            msg = consumer.try_next() => {
                                                // TODO: handle errors better
                                                if msg.is_err() {
                                                    break;
                                                }
                                                let msg = msg.unwrap();
                                                if msg.is_none() {
                                                    break;
                                                }
                                                let msg = msg.unwrap();
                                                let record = msg.deserialize();
                                                println!("got record: {:?}", record);
                                                sender.send(Box::new(record)).await.unwrap();
                                                // Check the collection id and write to the appropriate index
                                                // The abstraction we write to is called a "segment" and so we call
                                                // the appropriate segment provider to write the record
                                            }
                                        }
                                    }
                                });
                            }
                            for topic in net_old_assignments {
                                let cancellation_token = topic_to_cancel_token.get(topic);
                                if cancellation_token.is_some() {
                                    cancellation_token.unwrap().cancel();
                                }
                            }
                        }
                }
            }
        });
    }

    fn stop(&mut self) {
        if !self.running {
            return;
        }
        self.cancellation_token.cancel();
        // TODO: cancellation should also wait for the taksks to finish by joining them
        self.running = false;
    }
}

// A writer uses a memberlist_provider and a segment_provider to write to the index
// that is appropriate for the topic

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_writer() {
//         let (memberlist_sender, memberlist_receiver) = tokio::sync::broadcast::channel(10);
//         let assignment_policy = Arc::new(Mutex::new(RendezvousHashingAssignmentPolicy::new(
//             "default".to_string(),
//             "default".to_string(),
//         )));
//         let (msg_sender, msg_receiver) = tokio::sync::mpsc::channel(10);
//         let mut writer = Writer::new(memberlist_receiver, assignment_policy, msg_sender).await;
//         let memberlist = vec!["a".to_string(), "b".to_string(), "c".to_string()];
//         // tokio sleep
//         writer.start();
//         memberlist_sender.send(memberlist).unwrap();
//         tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
//         writer.stop();
//         return;
//     }
// }
