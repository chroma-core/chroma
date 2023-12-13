// use async_trait::async_trait;
// use futures::TryStreamExt;
// use kube::{
//     api::Api,
//     runtime::{watcher, WatchStreamExt},
//     Client, CustomResource,
// };
// use schemars::JsonSchema;
// use serde::{Deserialize, Serialize};
// use tokio::{pin, sync::broadcast::Sender};
// use tokio_util::sync::CancellationToken;

// trait Component {
//     fn start(&mut self);
//     fn stop(&mut self);
// }

// /* =========== Basic Types ============== */
// pub type Memberlist = Vec<String>;

// #[async_trait]
// pub(crate) trait MemberlistProvider {
//     async fn get_memberlist(&self) -> Memberlist;
// }

// /* =========== CRD ============== */
// #[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
// #[kube(
//     group = "chroma.cluster",
//     version = "v1",
//     kind = "MemberList",
//     root = "MemberListKubeResource",
//     namespaced
// )]
// pub(crate) struct MemberListCrd {
//     pub(crate) members: Vec<Member>,
// }

// // Define the structure for items in the members array
// #[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
// pub(crate) struct Member {
//     pub(crate) url: String,
// }

// /* =========== CR Provider ============== */
// pub(crate) struct CustomResourceMemberlistProvider {
//     memberlist_name: String,
//     kube_client: Client,
//     kube_ns: String,
//     memberlist_cr_client: Api<MemberListKubeResource>,
//     cancellation_token: CancellationToken, // TODO: cancellation token needs to be refreshed when we strt/stop
//     running: bool,
// }

// pub(crate) struct CustomResourceMemberlistProviderHandle {
//     channel: Sender<Memberlist>,
// }

// // TODO: parameterize namespace
// impl CustomResourceMemberlistProvider {
//     pub(crate) fn new(
//         memberlist_name: &str,
//         channel_send: Sender<Memberlist>,
//         kube_client: Client,
//         kube_ns: &str,
//     ) -> CustomResourceMemberlistProvider {
//         // Kube client is a buffer in tower, and cloning it is cheap -> https://docs.rs/tower/latest/tower/buffer/index.html
//         let api = Api::<MemberListKubeResource>::namespaced(kube_client.clone(), kube_ns);
//         let cancellation_token = CancellationToken::new();

//         let c: CustomResourceMemberlistProvider = CustomResourceMemberlistProvider {
//             memberlist_name: memberlist_name.to_string(),
//             kube_client: kube_client,
//             kube_ns: kube_ns.to_string(),
//             memberlist_cr_client: api,
//             cancellation_token: cancellation_token,
//             channel: channel_send,
//             running: false,
//         };
//         return c;
//     }
// }

// #[async_trait]
// impl MemberlistProvider for CustomResourceMemberlistProvider {
//     async fn get_memberlist(&self) -> Memberlist {
//         let memberlist = self.memberlist_cr_client.get(&self.memberlist_name).await;
//         match memberlist {
//             Ok(memberlist) => {
//                 let memberlist = memberlist.spec.members;
//                 let memberlist = memberlist
//                     .iter()
//                     .map(|member| member.url.clone())
//                     .collect::<Vec<String>>();
//                 return memberlist;
//             }
//             Err(err) => {
//                 // TODO: log memberlist error
//                 return vec![];
//             }
//         }
//     }
// }

// impl Component for CustomResourceMemberlistProvider {
//     fn start(&mut self) {
//         if self.running {
//             return;
//         }

//         // TODO: set running to true

//         let memberlist_cr_client =
//             Api::<MemberListKubeResource>::namespaced(self.kube_client.clone(), &self.kube_ns);
//         let cancellation_token = self.cancellation_token.clone();
//         let channel = self.channel.clone();

//         let stream = watcher(memberlist_cr_client, watcher::Config::default())
//             .default_backoff()
//             .applied_objects();

//         tokio::spawn(async move {
//             pin!(stream);

//             loop {
//                 // Get next or select cancellation token
//                 tokio::select! {
//                     _ = cancellation_token.cancelled() => {
//                         println!("Cancellation token cancelled");
//                         return;
//                     },
//                     event = stream.try_next() => {
//                         match event {
//                             Ok(event) => {
//                                 match event {
//                                     Some(event) => {
//                                         println!("Event: {:?}", event);
//                                         let memberlist = event.spec.members;
//                                         let memberlist = memberlist
//                                             .iter()
//                                             .map(|member| member.url.clone())
//                                             .collect::<Vec<String>>();
//                                         println!("Memberlist: {:?}", memberlist);
//                                         let _ = channel.send(memberlist);
//                                     },
//                                     None => {
//                                         println!("No event");
//                                     }
//                                 }
//                             },
//                             Err(err) => {
//                                 println!("Error: {}", err);
//                             }
//                         }
//                     }
//                 }
//             }
//         });
//     }

//     fn stop(&mut self) {
//         if !self.running {
//             return;
//         }
//         self.cancellation_token.cancel();
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     // #[tokio::test]
//     // async fn it_can_work() {
//     //     let (tx, mut rx) = tokio::sync::broadcast::channel(10); // TODO: what happens if capacity is exceeded?
//     //     let mut provider = CustomResourceMemberlistProvider::new("worker-memberlist", tx).await;
//     //     let list = provider.get_memberlist().await;
//     //     println!("list: {:?}", list);

//     //     provider.start();

//     //     // sleep to allow time for the watcher to get the initial state
//     //     tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;

//     //     let res = rx.recv().await.unwrap();
//     //     println!("GOT FROM CHANNEL: {:?}", res);

//     //     provider.stop();

//     //     tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
//     // }
// }
