use std::sync::Arc;
use std::{fmt::Debug, sync::RwLock};

use super::config::{CustomResourceMemberlistProviderConfig, MemberlistProviderConfig};
use crate::system::{Receiver, Sender};
use crate::{
    config::{Configurable, WorkerConfig},
    errors::{ChromaError, ErrorCodes},
    system::{Component, ComponentContext, Handler, StreamHandler},
};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::events::v1::Event;
use kube::{
    api::Api,
    config,
    runtime::{watcher, watcher::Error as WatchError, WatchStreamExt},
    Client, CustomResource,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/* =========== Basic Types ============== */
pub(crate) type Memberlist = Vec<String>;

#[async_trait]
pub(crate) trait MemberlistProvider: Component + Configurable {
    fn subscribe(&mut self, receiver: Box<dyn Receiver<Memberlist> + Send>) -> ();
}

/* =========== CRD ============== */
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "chroma.cluster",
    version = "v1",
    kind = "MemberList",
    root = "MemberListKubeResource",
    namespaced
)]
pub(crate) struct MemberListCrd {
    pub(crate) members: Vec<Member>,
}

// Define the structure for items in the members array
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub(crate) struct Member {
    pub(crate) url: String,
}

/* =========== CR Provider ============== */
pub(crate) struct CustomResourceMemberlistProvider {
    memberlist_name: String,
    kube_client: Client,
    kube_ns: String,
    memberlist_cr_client: Api<MemberListKubeResource>,
    queue_size: usize,
    current_memberlist: RwLock<Memberlist>,
    subscribers: Vec<Box<dyn Receiver<Memberlist> + Send>>,
}

impl Debug for CustomResourceMemberlistProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomResourceMemberlistProvider")
            .field("memberlist_name", &self.memberlist_name)
            .field("kube_ns", &self.kube_ns)
            .field("queue_size", &self.queue_size)
            .finish()
    }
}

#[derive(Error, Debug)]
pub(crate) enum CustomResourceMemberlistProviderConfigurationError {
    #[error("Failed to load kube client")]
    FailedToLoadKubeClient(#[from] kube::Error),
}

impl ChromaError for CustomResourceMemberlistProviderConfigurationError {
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            CustomResourceMemberlistProviderConfigurationError::FailedToLoadKubeClient(e) => {
                ErrorCodes::Internal
            }
        }
    }
}

#[async_trait]
impl Configurable for CustomResourceMemberlistProvider {
    async fn try_from_config(worker_config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
        let my_config = match &worker_config.memberlist_provider {
            MemberlistProviderConfig::CustomResource(config) => config,
        };
        let kube_client = match Client::try_default().await {
            Ok(client) => client,
            Err(err) => {
                return Err(Box::new(
                    CustomResourceMemberlistProviderConfigurationError::FailedToLoadKubeClient(err),
                ))
            }
        };
        let memberlist_cr_client = Api::<MemberListKubeResource>::namespaced(
            kube_client.clone(),
            &worker_config.kube_namespace,
        );

        let c: CustomResourceMemberlistProvider = CustomResourceMemberlistProvider {
            memberlist_name: my_config.memberlist_name.clone(),
            kube_ns: worker_config.kube_namespace.clone(),
            kube_client: kube_client,
            memberlist_cr_client: memberlist_cr_client,
            queue_size: my_config.queue_size,
            current_memberlist: RwLock::new(vec![]),
            subscribers: vec![],
        };
        Ok(c)
    }
}

impl CustomResourceMemberlistProvider {
    fn new(
        memberlist_name: String,
        kube_client: Client,
        kube_ns: String,
        queue_size: usize,
    ) -> Self {
        let memberlist_cr_client =
            Api::<MemberListKubeResource>::namespaced(kube_client.clone(), &kube_ns);
        CustomResourceMemberlistProvider {
            memberlist_name: memberlist_name,
            kube_ns: kube_ns,
            kube_client: kube_client,
            memberlist_cr_client: memberlist_cr_client,
            queue_size: queue_size,
            current_memberlist: RwLock::new(vec![]),
            subscribers: vec![],
        }
    }

    fn connect_to_kube_stream(&self, ctx: &ComponentContext<CustomResourceMemberlistProvider>) {
        let memberlist_cr_client =
            Api::<MemberListKubeResource>::namespaced(self.kube_client.clone(), &self.kube_ns);

        let stream = watcher(memberlist_cr_client, watcher::Config::default())
            .default_backoff()
            .applied_objects();
        let stream = stream.then(|event| async move {
            match event {
                Ok(event) => {
                    let event = event;
                    println!("Kube stream event: {:?}", event);
                    Some(event)
                }
                Err(err) => {
                    println!("Error acquiring memberlist: {}", err);
                    None
                }
            }
        });
        self.register_stream(stream, ctx);
    }

    async fn notify_subscribers(&self) -> () {
        let curr_memberlist = match self.current_memberlist.read() {
            Ok(curr_memberlist) => curr_memberlist.clone(),
            Err(err) => {
                // TODO: Log error and attempt recovery
                return;
            }
        };

        for subscriber in self.subscribers.iter() {
            let _ = subscriber.send(curr_memberlist.clone()).await;
        }
    }
}

#[async_trait]
impl Component for CustomResourceMemberlistProvider {
    fn queue_size(&self) -> usize {
        self.queue_size
    }

    async fn on_start(&mut self, ctx: &ComponentContext<CustomResourceMemberlistProvider>) {
        self.connect_to_kube_stream(ctx);
    }
}

#[async_trait]
impl Handler<Option<MemberListKubeResource>> for CustomResourceMemberlistProvider {
    async fn handle(
        &mut self,
        event: Option<MemberListKubeResource>,
        _ctx: &ComponentContext<CustomResourceMemberlistProvider>,
    ) {
        match event {
            Some(memberlist) => {
                println!("Memberlist event in CustomResourceMemberlistProvider. Name: {:?}. Members: {:?}", memberlist.metadata.name, memberlist.spec.members);
                let name = match &memberlist.metadata.name {
                    Some(name) => name,
                    None => {
                        // TODO: Log an error
                        return;
                    }
                };
                if name != &self.memberlist_name {
                    return;
                }
                let memberlist = memberlist.spec.members;
                let memberlist = memberlist
                    .iter()
                    .map(|member| member.url.clone())
                    .collect::<Vec<String>>();
                {
                    let curr_memberlist_handle = self.current_memberlist.write();
                    match curr_memberlist_handle {
                        Ok(mut curr_memberlist) => {
                            *curr_memberlist = memberlist;
                        }
                        Err(err) => {
                            // TODO: Log an error
                        }
                    }
                }
                // Inform subscribers
                self.notify_subscribers().await;
            }
            None => {
                // Stream closed or error
            }
        }
    }
}

impl StreamHandler<Option<MemberListKubeResource>> for CustomResourceMemberlistProvider {}

#[async_trait]
impl MemberlistProvider for CustomResourceMemberlistProvider {
    fn subscribe(&mut self, sender: Box<dyn Receiver<Memberlist> + Send>) -> () {
        self.subscribers.push(sender);
    }
}

#[cfg(test)]
mod tests {
    use crate::system::System;

    use super::*;

    #[tokio::test]
    #[cfg(CHROMA_KUBERNETES_INTEGRATION)]
    async fn it_can_work() {
        // TODO: This only works if you have a kubernetes cluster running locally with a memberlist
        // We need to implement a test harness for this. For now, it will silently do nothing
        // if you don't have a kubernetes cluster running locally and only serve as a reminder
        // and demonstration of how to use the memberlist provider.
        let kube_ns = "chroma".to_string();
        let kube_client = Client::try_default().await.unwrap();
        let memberlist_provider = CustomResourceMemberlistProvider::new(
            "worker-memberlist".to_string(),
            kube_client.clone(),
            kube_ns.clone(),
            10,
        );
        let mut system = System::new();
        let handle = system.start_component(memberlist_provider);
    }
}
