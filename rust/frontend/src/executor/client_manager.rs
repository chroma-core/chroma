use async_trait::async_trait;
use chroma_memberlist::memberlist_provider::Memberlist;
use chroma_system::{Component, ComponentContext, Handler};
use chroma_types::chroma_proto::query_executor_client::QueryExecutorClient;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tonic::transport::{Channel, Endpoint};
use tower::discover::Change;

#[derive(Debug)]
pub(super) struct ClientManager {
    // The name of the node to the grpc client
    node_name_to_client:
        Arc<Mutex<HashMap<String, QueryExecutorClient<tonic::transport::Channel>>>>,
    // The name of the node to the sender to the channel to add / remove the ip
    node_name_to_change_sender:
        HashMap<String, tokio::sync::mpsc::Sender<Change<String, Endpoint>>>,
    connections_per_node: usize,
    old_memberlist: Memberlist,
}

impl ClientManager {
    pub(super) fn new(
        node_name_to_client: Arc<
            Mutex<HashMap<String, QueryExecutorClient<tonic::transport::Channel>>>,
        >,
        connections_per_node: usize,
    ) -> Self {
        ClientManager {
            node_name_to_client,
            node_name_to_change_sender: HashMap::new(),
            connections_per_node,
            old_memberlist: Memberlist::new(),
        }
    }

    async fn remove_ip_for_node(&self, ip: String, node: &str) {
        let sender = match self.node_name_to_change_sender.get(&ip) {
            Some(sender) => sender,
            None => {
                // There is no one to return the error to, so just log it
                tracing::error!("Failed to find sender for node: {:?}", node);
                return;
            }
        };

        match sender.send(Change::Remove(ip)).await {
            Ok(_) => {}
            Err(e) => {
                // There is no one to return the error to, so just log it
                tracing::error!("Failed to remove ip from client manager: {:?}", e);
            }
        }
    }

    async fn add_ip_for_node(&mut self, ip: String, node: &str) {
        let endpoint = match Endpoint::from_shared(ip.clone()) {
            Ok(endpoint) => endpoint,
            Err(e) => {
                // There is no one to return the error to, so just log it
                tracing::error!("Failed to create endpoint from ip: {:?}", e);
                return;
            }
        };

        let sender = match self.node_name_to_change_sender.get(node) {
            Some(sender) => sender.clone(),
            None => {
                // TODO(hammadb): configure timeouts and such
                let (chan, channel_change_sender) =
                    Channel::balance_channel::<String>(self.connections_per_node);
                let client = QueryExecutorClient::new(chan);

                // TODO: insert up to the max number of connections
                let mut node_name_to_client_guard = self.node_name_to_client.lock();
                node_name_to_client_guard.insert(node.to_string(), client);
                self.node_name_to_change_sender
                    .insert(node.to_string(), channel_change_sender.clone());
                channel_change_sender
            }
        };

        match sender.send(Change::Insert(ip, endpoint)).await {
            Ok(_) => {}
            Err(e) => {
                // There is no one to return the error to, so just log it
                tracing::error!("Failed to add ip to client manager: {:?}", e);
            }
        }
    }
}

///////////////////////// Component Impl /////////////////////////

impl Component for ClientManager {
    fn get_name() -> &'static str {
        "ClientManger"
    }

    fn queue_size(&self) -> usize {
        1000
    }
}

#[async_trait]
impl Handler<Memberlist> for ClientManager {
    type Result = ();

    async fn handle(&mut self, new_members: Memberlist, _ctx: &ComponentContext<ClientManager>) {
        let mut old_node_to_ip = HashMap::new();
        for old_member in self.old_memberlist.iter() {
            old_node_to_ip.insert(
                old_member.member_node_name.to_string(),
                old_member.member_ip.to_string(),
            );
        }

        let mut seen_nodes = HashSet::new();
        for new_member in new_members.iter() {
            let node = new_member.member_node_name.as_str();
            let ip = new_member.member_ip.as_str();

            match old_node_to_ip.get(node) {
                Some(old_ip) => {
                    if *old_ip != ip {
                        // The ip has changed
                        self.remove_ip_for_node(old_ip.to_string(), node).await;
                        self.add_ip_for_node(ip.to_string(), node).await;
                    }
                }
                None => {
                    // This is a new node
                    self.add_ip_for_node(ip.to_string(), node).await;
                }
            }
            seen_nodes.insert(node.to_string());
        }

        for (node, ip) in old_node_to_ip.iter() {
            if !seen_nodes.contains(node) {
                // This node has been removed
                self.remove_ip_for_node(ip.to_string(), node).await;
            }
        }
    }
}
