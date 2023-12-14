// use super::config::{CustomResourceMemberlistProviderConfig, MemberlistProviderConfig};
// use crate::{
//     config::{Configurable, WorkerConfig},
//     errors::{ChromaError, ErrorCodes},
//     system::{Component, StreamHandler},
// };
// use async_trait::async_trait;
// use futures::TryStreamExt;
// use kube::{
//     api::Api,
//     config,
//     runtime::{watcher, WatchStreamExt},
//     Client, CustomResource,
// };
// use schemars::JsonSchema;
// use serde::{Deserialize, Serialize};
// use thiserror::Error;
// use tokio::{pin, sync::broadcast::Sender};
// use tokio_util::sync::CancellationToken;

// /* =========== Basic Types ============== */
// pub type Memberlist = Vec<String>;

// #[async_trait]
// pub(crate) trait MemberlistProvider: Component {
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
//     queue_size: usize,
// }

// #[derive(Error, Debug)]
// pub(crate) enum CustomResourceMemberlistProviderConfigurationError {
//     #[error("Failed to load kube client")]
//     FailedToLoadKubeClient(#[from] kube::Error),
// }

// impl ChromaError for CustomResourceMemberlistProviderConfigurationError {
//     fn code(&self) -> crate::errors::ErrorCodes {
//         match self {
//             CustomResourceMemberlistProviderConfigurationError::FailedToLoadKubeClient(e) => {
//                 ErrorCodes::Internal
//             }
//         }
//     }
// }

// #[async_trait]
// impl Configurable for CustomResourceMemberlistProvider {
//     async fn try_from_config(worker_config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
//         let my_config = match &worker_config.memberlist_provider {
//             MemberlistProviderConfig::CustomResource(config) => config,
//         };
//         let kube_client = match Client::try_default().await {
//             Ok(client) => client,
//             Err(err) => {
//                 return Err(Box::new(
//                     CustomResourceMemberlistProviderConfigurationError::FailedToLoadKubeClient(err),
//                 ))
//             }
//         };
//         let memberlist_cr_client = Api::<MemberListKubeResource>::namespaced(
//             kube_client.clone(),
//             &worker_config.kube_namespace,
//         );

//         let c: CustomResourceMemberlistProvider = CustomResourceMemberlistProvider {
//             memberlist_name: my_config.memberlist_name.clone(),
//             kube_ns: worker_config.kube_namespace.clone(),
//             kube_client: kube_client,
//             memberlist_cr_client: memberlist_cr_client,
//             queue_size: my_config.queue_size,
//         };
//         Ok(c)
//     }
// }

// // #[async_trait]
// // impl MemberlistProvider<CustomResourceMemberlistProviderConfig>
// //     for CustomResourceMemberlistProvider
// // {
// //     fn new(config: &CustomResourceMemberlistProviderConfig) -> CustomResourceMemberlistProvider {
// //         // Kube client is a buffer in tower, and cloning it is cheap -> https://docs.rs/tower/latest/tower/buffer/index.html
// //         let api = Api::<MemberListKubeResource>::namespaced(kube_client.clone(), &kube_ns);

// //         let c: CustomResourceMemberlistProvider = CustomResourceMemberlistProvider {
// //             memberlist_name: config.memberlist_name,
// //             kube_ns: config.kub,
// //             memberlist_cr_client: api,
// //         };
// //         return c;
// //     }

// //     async fn get_memberlist(&self) -> Memberlist {
// //         let memberlist = self.memberlist_cr_client.get(&self.memberlist_name).await;
// //         match memberlist {
// //             Ok(memberlist) => {
// //                 let memberlist = memberlist.spec.members;
// //                 let memberlist = memberlist
// //                     .iter()
// //                     .map(|member| member.url.clone())
// //                     .collect::<Vec<String>>();
// //                 return memberlist;
// //             }
// //             Err(err) => {}
// //         }
// //     }
// // }

// impl Component for CustomResourceMemberlistProvider {
//     fn queue_size(&self) -> usize {
//         self.queue_size
//     }
// }

// impl StreamHandler<Result<Event>> for CustomResourceMemberlistProvider {
//     fn handle(&self, _message: ()) {}
// }

// impl CustomResourceMemberlistProvider {
//     fn connect_to_kube_stream(&self) {
//         let memberlist_cr_client =
//             Api::<MemberListKubeResource>::namespaced(self.kube_client.clone(), &self.kube_ns);

//         let stream = watcher(memberlist_cr_client, watcher::Config::default())
//             .default_backoff()
//             .applied_objects();
//         self.register_stream(stream);
//     }
// }

// // #[async_trait]
// // impl MemberlistProvider for CustomResourceMemberlistProvider {
// //     async fn get_memberlist(&self) -> Memberlist {
// //         let memberlist = self.memberlist_cr_client.get(&self.memberlist_name).await;
// //         match memberlist {
// //             Ok(memberlist) => {
// //                 let memberlist = memberlist.spec.members;
// //                 let memberlist = memberlist
// //                     .iter()
// //                     .map(|member| member.url.clone())
// //                     .collect::<Vec<String>>();
// //                 return memberlist;
// //             }
// //             Err(err) => {
// //                 // TODO: log memberlist error
// //                 return vec![];
// //             }
// //         }
// //     }
// // }

// // impl Component for CustomResourceMemberlistProvider {
// //     fn start(&mut self) {
// //         if self.running {
// //             return;
// //         }

// //         // TODO: set running to true

// //         let memberlist_cr_client =
// //             Api::<MemberListKubeResource>::namespaced(self.kube_client.clone(), &self.kube_ns);
// //         let cancellation_token = self.cancellation_token.clone();
// //         let channel = self.channel.clone();

// //         let stream = watcher(memberlist_cr_client, watcher::Config::default())
// //             .default_backoff()
// //             .applied_objects();

// //         tokio::spawn(async move {
// //             pin!(stream);

// //             loop {
// //                 // Get next or select cancellation token
// //                 tokio::select! {
// //                     _ = cancellation_token.cancelled() => {
// //                         println!("Cancellation token cancelled");
// //                         return;
// //                     },
// //                     event = stream.try_next() => {
// //                         match event {
// //                             Ok(event) => {
// //                                 match event {
// //                                     Some(event) => {
// //                                         println!("Event: {:?}", event);
// //                                         let memberlist = event.spec.members;
// //                                         let memberlist = memberlist
// //                                             .iter()
// //                                             .map(|member| member.url.clone())
// //                                             .collect::<Vec<String>>();
// //                                         println!("Memberlist: {:?}", memberlist);
// //                                         let _ = channel.send(memberlist);
// //                                     },
// //                                     None => {
// //                                         println!("No event");
// //                                     }
// //                                 }
// //                             },
// //                             Err(err) => {
// //                                 println!("Error: {}", err);
// //                             }
// //                         }
// //                     }
// //                 }
// //             }
// //         });
// //     }

// //     fn stop(&mut self) {
// //         if !self.running {
// //             return;
// //         }
// //         self.cancellation_token.cancel();
// //     }
// // }

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
