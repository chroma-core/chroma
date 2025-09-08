use super::config::MemberlistProviderConfig;
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{Component, ComponentContext, Handler, ReceiverForMessage, StreamHandler};
use futures::StreamExt;
use kube::runtime::watcher::Config;
use kube::{
    api::Api,
    runtime::{watcher, WatchStreamExt},
    Client, CustomResource,
};
use parking_lot::RwLock;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

/* =========== Basic Types ============== */
pub type Memberlist = Vec<Member>;

#[async_trait]
pub trait MemberlistProvider: Component + Configurable<MemberlistProviderConfig> {
    fn subscribe(&mut self, receiver: Box<dyn ReceiverForMessage<Memberlist> + Send>);
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
    pub(crate) members: Option<Vec<Member>>,
}

/// A member in a memberlist represents a kubernetes pod
/// who's been deemed eligible to participate in the memberlist
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Member {
    // The ID of the member
    pub member_id: String,
    // The IP address of the member
    pub member_ip: String,
    // The k8s node name of the member
    pub member_node_name: String,
}

/* =========== CR Provider ============== */
pub struct CustomResourceMemberlistProvider {
    memberlist_name: String,
    kube_client: Client,
    kube_ns: String,
    #[allow(dead_code)]
    memberlist_cr_client: Api<MemberListKubeResource>,
    queue_size: usize,
    current_memberlist: RwLock<Memberlist>,
    subscribers: Vec<Box<dyn ReceiverForMessage<Memberlist> + Send>>,
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
    fn code(&self) -> ErrorCodes {
        match self {
            CustomResourceMemberlistProviderConfigurationError::FailedToLoadKubeClient(_e) => {
                ErrorCodes::Internal
            }
        }
    }
}

#[async_trait]
impl Configurable<MemberlistProviderConfig> for CustomResourceMemberlistProvider {
    async fn try_from_config(
        config: &MemberlistProviderConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let MemberlistProviderConfig::CustomResource(my_config) = &config;
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
            &my_config.kube_namespace,
        );

        let c: CustomResourceMemberlistProvider = CustomResourceMemberlistProvider {
            memberlist_name: my_config.memberlist_name.clone(),
            kube_ns: my_config.kube_namespace.clone(),
            kube_client,
            memberlist_cr_client,
            queue_size: my_config.queue_size,
            current_memberlist: RwLock::new(vec![]),
            subscribers: vec![],
        };
        Ok(c)
    }
}

impl CustomResourceMemberlistProvider {
    // This is not reserved for testing.  If you need to use it outside test contexts, remove this
    // line.  It exists solely to satisfy the linter.
    #[cfg(test)]
    fn new(
        memberlist_name: String,
        kube_client: Client,
        kube_ns: String,
        queue_size: usize,
    ) -> Self {
        let memberlist_cr_client =
            Api::<MemberListKubeResource>::namespaced(kube_client.clone(), &kube_ns);
        CustomResourceMemberlistProvider {
            memberlist_name,
            kube_ns,
            kube_client,
            memberlist_cr_client,
            queue_size,
            current_memberlist: RwLock::new(vec![]),
            subscribers: vec![],
        }
    }

    fn connect_to_kube_stream(&self, ctx: &ComponentContext<CustomResourceMemberlistProvider>) {
        let memberlist_cr_client =
            Api::<MemberListKubeResource>::namespaced(self.kube_client.clone(), &self.kube_ns);

        let field_selector = format!("metadata.name={}", self.memberlist_name);
        let conifg = Config::default().fields(&field_selector);

        let stream = watcher(memberlist_cr_client, conifg)
            .default_backoff()
            .applied_objects();
        let stream = stream.then(|event| async move {
            match event {
                Ok(event) => Some(event),
                Err(err) => {
                    tracing::error!("Error acquiring memberlist: {}", err);
                    None
                }
            }
        });
        self.register_stream(stream, ctx);
    }

    async fn notify_subscribers(&self) {
        let curr_memberlist = self.current_memberlist.read().clone();

        for subscriber in self.subscribers.iter() {
            let _ = subscriber.send(curr_memberlist.clone(), None).await;
        }
    }
}

#[async_trait]
impl Component for CustomResourceMemberlistProvider {
    fn get_name() -> &'static str {
        "Custom resource member list provider"
    }

    fn queue_size(&self) -> usize {
        self.queue_size
    }

    async fn on_start(&mut self, ctx: &ComponentContext<CustomResourceMemberlistProvider>) {
        self.connect_to_kube_stream(ctx);
    }
}

#[async_trait]
impl Handler<Option<MemberListKubeResource>> for CustomResourceMemberlistProvider {
    type Result = ();

    async fn handle(
        &mut self,
        event: Option<MemberListKubeResource>,
        _ctx: &ComponentContext<CustomResourceMemberlistProvider>,
    ) {
        match event {
            Some(memberlist) => {
                tracing::info!("Memberlist event in CustomResourceMemberlistProvider. Name: {:?}. Members: {:?}", memberlist.metadata.name, memberlist.spec.members);
                let name = match &memberlist.metadata.name {
                    Some(name) => name,
                    None => {
                        tracing::error!("Memberlist event without memberlist name");
                        return;
                    }
                };
                if name != &self.memberlist_name {
                    return;
                }

                let memberlist = match memberlist.spec.members {
                    Some(members) => members,
                    None => {
                        // We could change `members` in `MemberListCrd` to `Vec<Member>` instead of `Option<Vec<Member>>`. However, then the deserialization fails until the SysDb populates the memberlist for the first time because Kubernetes initially creates the CRD with no members. If the deserialization fails, the watcher stream backs off and retries--which is not what we want as it causes increased startup latency (10-20s) when running the system locally.
                        tracing::warn!("Memberlist has not yet been populated");
                        return;
                    }
                };

                {
                    let mut curr_memberlist_handle = self.current_memberlist.write();
                    *curr_memberlist_handle = memberlist.clone();
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
    fn subscribe(&mut self, sender: Box<dyn ReceiverForMessage<Memberlist> + Send>) {
        self.subscribers.push(sender);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_system::System;
    use std::sync::Arc;

    #[derive(Debug)]
    struct MemberlistSubscriber {
        memberlist: Arc<RwLock<Option<Memberlist>>>,
    }

    impl Component for MemberlistSubscriber {
        fn queue_size(&self) -> usize {
            10
        }

        fn get_name() -> &'static str {
            "Memberlist subscriber"
        }
    }

    #[async_trait]
    impl Handler<Memberlist> for MemberlistSubscriber {
        type Result = ();

        async fn handle(
            &mut self,
            event: Memberlist,
            _ctx: &ComponentContext<MemberlistSubscriber>,
        ) {
            let mut memberlist = self.memberlist.write();
            *memberlist = Some(event);
        }
    }

    #[tokio::test]
    // Naming this "test_k8s_integration_" means that the Tilt stack is required. See rust/worker/README.md.
    async fn test_k8s_integration_it_can_work() {
        // TODO: This only works if you have a kubernetes cluster running locally with a memberlist
        // We need to implement a test harness for this. For now, it will silently do nothing
        // if you don't have a kubernetes cluster running locally and only serve as a reminder
        // and demonstration of how to use the memberlist provider.

        let system = System::new();

        let kube_ns = "chroma".to_string();
        let kube_client = Client::try_default().await.unwrap();
        let mut memberlist_provider = CustomResourceMemberlistProvider::new(
            "query-service-memberlist".to_string(),
            kube_client.clone(),
            kube_ns.clone(),
            10,
        );

        let debug_memberlist = Arc::new(RwLock::new(None));
        let subscriber = MemberlistSubscriber {
            memberlist: debug_memberlist.clone(),
        };
        let subscriber_handle = system.start_component(subscriber);
        memberlist_provider.subscribe(subscriber_handle.receiver());
        let _handle = system.start_component(memberlist_provider);

        // Wait for a while to let the stream run
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let memberlist = debug_memberlist.read();
        assert!(memberlist.is_some());
        let memberlist = memberlist.as_ref().unwrap();
        // The query service memberlist in our test tilt config has two nodes
        assert_eq!(memberlist.len(), 2);
        // The ids should be formatted as "query-service-<node number>"
        for member in memberlist.iter() {
            assert!(member.member_id.starts_with("query-service-"));
        }
    }
}
