// A scheduler recieves embedding records for a given batch of documents
// and schedules them to be ingested to the segment manager

use crate::{
    system::{Component, ComponentContext, Handler, Receiver},
    types::EmbeddingRecord,
};
use async_trait::async_trait;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter, Result},
    sync::{atomic::AtomicBool, Arc},
};
use tokio::{io::Empty, select};

pub(crate) struct RoundRobinScheduler {
    // The segment manager to schedule to, a segment manager is a component
    // segment_manager: SegmentManager
    curr_wake_up: Option<tokio::sync::oneshot::Sender<WakeMessage>>,
    tenant_to_queue: HashMap<String, tokio::sync::mpsc::Sender<Arc<EmbeddingRecord>>>,
    new_tenant_channel: Option<tokio::sync::mpsc::Sender<NewTenantMessage>>,
}

impl Debug for RoundRobinScheduler {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("Scheduler").finish()
    }
}

impl RoundRobinScheduler {
    pub(crate) fn new() -> Self {
        RoundRobinScheduler {
            curr_wake_up: None,
            tenant_to_queue: HashMap::new(),
            new_tenant_channel: None,
        }
    }
}

impl Component for RoundRobinScheduler {
    fn queue_size(&self) -> usize {
        1000
    }

    fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        println!("Starting scheduler");
        let sleep_sender = ctx.sender.clone();
        let (new_tenant_tx, mut new_tenant_rx) = tokio::sync::mpsc::channel(1000);
        self.new_tenant_channel = Some(new_tenant_tx);
        let cancellation_token = ctx.cancellation_token.clone();
        tokio::spawn(async move {
            let mut tenant_queues: HashMap<
                String,
                tokio::sync::mpsc::Receiver<Arc<EmbeddingRecord>>,
            > = HashMap::new();
            loop {
                // TODO: handle cancellation
                let mut did_work = false;
                for tenant_queue in tenant_queues.values_mut() {
                    match tenant_queue.try_recv() {
                        Ok(message) => {
                            // TODO: Send the message to the segment manager in a spawned task
                            // so we can continue polling
                            did_work = true;
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                            continue;
                        }
                        Err(_) => {
                            // TODO: Handle a erroneous channel
                            // log an error
                            continue;
                        }
                    };
                }

                match new_tenant_rx.try_recv() {
                    Ok(new_tenant_message) => {
                        tenant_queues.insert(new_tenant_message.tenant, new_tenant_message.channel);
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        // no - op
                    }
                    Err(_) => {
                        // TODO: handle erroneous channel
                        // log an error
                        continue;
                    }
                };

                if !did_work {
                    // Send a sleep message to the sender
                    let (wake_tx, wake_rx) = tokio::sync::oneshot::channel();
                    let sleep_res = sleep_sender.send(SleepMessage { sender: wake_tx }).await;
                    let wake_res = wake_rx.await;
                    println!("Scheduler awake");
                }
            }
        });
    }
}

#[async_trait]
impl Handler<(String, Arc<EmbeddingRecord>)> for RoundRobinScheduler {
    async fn handle(
        &mut self,
        message: (String, Arc<EmbeddingRecord>),
        _ctx: &ComponentContext<Self>,
    ) {
        let (tenant, embedding_record) = message;
        // Check if the tenant is already in the tenant set, if not we need to inform the scheduler loop
        // of a new tenant
        if self.tenant_to_queue.get(&tenant).is_none() {
            // Create a new channel for the tenant
            let (sender, reciever) = tokio::sync::mpsc::channel(1000);
            // Add the tenant to the tenant set
            self.tenant_to_queue.insert(tenant.clone(), sender);
            // Send the new tenant message to the scheduler loop
            let new_tenant_channel = match self.new_tenant_channel {
                Some(ref mut channel) => channel,
                None => {
                    // TODO: this is an error
                    // It should always be populated by on_start
                    return;
                }
            };
            let res = new_tenant_channel
                .send(NewTenantMessage {
                    tenant: tenant.clone(),
                    channel: reciever,
                })
                .await;
            // TODO: handle this res
        }

        // Send the embedding record to the tenant's channel
        let res = self
            .tenant_to_queue
            .get(&tenant)
            .unwrap()
            .send(embedding_record)
            .await;
        // TODO: handle this res

        // Check if the scheduler is sleeping, if so wake it up
        if self.curr_wake_up.is_some() {
            // Send a wake up message to the scheduler loop
            let res = self.curr_wake_up.take().unwrap().send(WakeMessage {});
            // TOOD: handle this res
        }
    }
}

#[async_trait]
impl Handler<SleepMessage> for RoundRobinScheduler {
    async fn handle(&mut self, message: SleepMessage, _ctx: &ComponentContext<Self>) {
        // Set the current wake up channel
        self.curr_wake_up = Some(message.sender);
    }
}

/// Used by round robin scheduler to wake its scheduler loop
#[derive(Debug)]
struct WakeMessage {}

/// The round robin scheduler will sleep when there is no work to be done and send a sleep message
/// this allows the manager to wake it up when there is work to be scheduled
#[derive(Debug)]
struct SleepMessage {
    sender: tokio::sync::oneshot::Sender<WakeMessage>,
}

struct NewTenantMessage {
    tenant: String,
    channel: tokio::sync::mpsc::Receiver<Arc<EmbeddingRecord>>,
}
