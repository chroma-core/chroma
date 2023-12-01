use std::mem;

use crate::Component;
use async_trait::async_trait;
use futures::TryStreamExt;
use kube::{
    api::Api,
    runtime::{watcher, WatchStreamExt},
    Client, CustomResource,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::{pin, sync::broadcast::Sender};
use tokio_util::sync::CancellationToken;

/* =========== Basic Types ============== */

pub type Memberlist = Vec<String>;

#[async_trait]
pub trait MemberlistProvider: Component {
    async fn get_memberlist(&self) -> Memberlist;
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
pub struct MemberListCrd {
    pub members: Vec<Member>,
}

// Define the structure for items in the members array
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Member {
    pub url: String,
}

pub struct CustomResourceMemberlistProvider {
    memberlist_name: String,
    kube_client: Client,
    memberlist_cr_client: Api<MemberListKubeResource>,
    cancellation_token: CancellationToken,
    channel: Sender<Memberlist>,
    running: bool,
}

// TODO: parameterize namespace
impl CustomResourceMemberlistProvider {
    // TODO: implement builder pattern so that this is not async
    pub async fn new(
        memberlist_name: &str,
        channel_send: Sender<Memberlist>,
    ) -> CustomResourceMemberlistProvider {
        let kube_client = Client::try_default().await;

        if kube_client.is_err() {
            // TODO: don't panic
            panic!(
                "kube_client to create kube client: {}",
                kube_client.err().unwrap()
            );
        }
        // TODO: pattern match instead of unwrap
        let kube_client = kube_client.unwrap();
        // Kube client is a buffer in tower, and cloning it is cheap -> https://docs.rs/tower/latest/tower/buffer/index.html
        let api = Api::<MemberListKubeResource>::namespaced(kube_client.clone(), "chroma");
        let cancellation_token = CancellationToken::new();

        let c: CustomResourceMemberlistProvider = CustomResourceMemberlistProvider {
            memberlist_name: memberlist_name.to_string(),
            kube_client: kube_client,
            memberlist_cr_client: api,
            cancellation_token: cancellation_token,
            channel: channel_send,
            running: false,
        };
        return c;
    }
}

#[async_trait]
impl MemberlistProvider for CustomResourceMemberlistProvider {
    async fn get_memberlist(&self) -> Memberlist {
        let memberlist = self.memberlist_cr_client.get(&self.memberlist_name).await;
        if memberlist.is_err() {
            panic!("Failed to get memberlist: {}", memberlist.err().unwrap());
        }
        let memberlist = memberlist.unwrap();
        let memberlist = memberlist.spec.members;
        let memberlist = memberlist
            .iter()
            .map(|member| member.url.clone())
            .collect::<Vec<String>>();
        return memberlist;
    }
}

impl Component for CustomResourceMemberlistProvider {
    fn start(&self) {
        if self.running {
            return;
        }

        let memberlist_cr_client =
            Api::<MemberListKubeResource>::namespaced(self.kube_client.clone(), "chroma");
        let cancellation_token = self.cancellation_token.clone();
        let channel = self.channel.clone();

        let stream = watcher(memberlist_cr_client, watcher::Config::default())
            .default_backoff()
            .applied_objects();

        tokio::spawn(async move {
            pin!(stream);

            loop {
                // Get next or select cancellation token
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        println!("Cancellation token cancelled");
                        return;
                    },
                    event = stream.try_next() => {
                        match event {
                            Ok(event) => {
                                match event {
                                    Some(event) => {
                                        println!("Event: {:?}", event);
                                        let memberlist = event.spec.members;
                                        let memberlist = memberlist
                                            .iter()
                                            .map(|member| member.url.clone())
                                            .collect::<Vec<String>>();
                                        println!("Memberlist: {:?}", memberlist);
                                        let _ = channel.send(memberlist);
                                    },
                                    None => {
                                        println!("No event");
                                    }
                                }
                            },
                            Err(err) => {
                                println!("Error: {}", err);
                            }
                        }
                    }
                }
            }
        });
    }

    fn stop(&self) {
        if !self.running {
            return;
        }
        self.cancellation_token.cancel();
    }
}
