use async_trait::async_trait;
use chroma_memberlist::memberlist_provider::Memberlist;
use chroma_system::{Component, ComponentContext, Handler};
use chroma_types::chroma_proto::query_executor_client::QueryExecutorClient;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tonic::transport::{Channel, Endpoint};
use tower::discover::Change;

/// A component that manages the gRPC clients for the query executors
/// # Fields
/// - `node_name_to_client` - A map from the node name to the gRPC client
/// - `node_name_to_change_sender` - A map from the node name to the sender to the channel to add / remove the ip
/// - `connections_per_node` - The number of connections to maintain per node
/// - `old_memberlist` - The old memberlist to compare against
/// # Notes
/// The client manager is responsible for creating and maintaining the gRPC clients for the query nodes.
/// It listens for changes to the memberlist and updates the clients accordingly.
#[derive(Debug)]
pub(super) struct ClientManager {
    // The name of the node to the grpc client
    node_name_to_client:
        Arc<RwLock<HashMap<String, QueryExecutorClient<tonic::transport::Channel>>>>,
    // The name of the node to the sender to the channel to add / remove the ip
    node_name_to_change_sender:
        HashMap<String, tokio::sync::mpsc::Sender<Change<String, Endpoint>>>,
    connections_per_node: usize,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
    old_memberlist: Memberlist,
}

impl ClientManager {
    pub(super) fn new(
        node_name_to_client: Arc<
            RwLock<HashMap<String, QueryExecutorClient<tonic::transport::Channel>>>,
        >,
        connections_per_node: usize,
        connect_timeout_ms: u64,
        request_timeout_ms: u64,
    ) -> Self {
        ClientManager {
            node_name_to_client,
            node_name_to_change_sender: HashMap::new(),
            connections_per_node,
            connect_timeout_ms,
            request_timeout_ms,
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

        for i in 0..self.connections_per_node {
            let indexed_connection_id = Self::indexed_connection_id(node, i);
            match sender.send(Change::Remove(indexed_connection_id)).await {
                Ok(_) => {}
                Err(e) => {
                    // There is no one to return the error to, so just log it
                    tracing::error!("Failed to remove ip from client manager: {:?}", e);
                }
            }
        }
    }

    async fn add_ip_for_node(&mut self, ip: String, node: &str) {
        // TODO: Configure the port
        let ip_with_port = format!("http://{}:{}", ip, 50051);
        let endpoint = match Endpoint::from_shared(ip_with_port) {
            Ok(endpoint) => endpoint
                .connect_timeout(std::time::Duration::from_millis(self.connect_timeout_ms))
                .timeout(std::time::Duration::from_millis(self.request_timeout_ms)),
            Err(e) => {
                // There is no one to return the error to, so just log it
                tracing::error!("Failed to create endpoint from ip: {:?}", e);
                return;
            }
        };

        let sender = match self.node_name_to_change_sender.get(node) {
            Some(sender) => sender.clone(),
            None => {
                let (chan, channel_change_sender) =
                    Channel::balance_channel::<String>(self.connections_per_node);
                let client = QueryExecutorClient::new(chan);

                let mut node_name_to_client_guard = self.node_name_to_client.write();
                node_name_to_client_guard.insert(node.to_string(), client);
                self.node_name_to_change_sender
                    .insert(node.to_string(), channel_change_sender.clone());
                channel_change_sender
            }
        };

        for i in 0..self.connections_per_node {
            // Append the index to the node name to make it unique, otherwise
            // the channel will be overwritten and we will only have one connection
            let indexed_connection_id = Self::indexed_connection_id(node, i);
            match sender
                .send(Change::Insert(indexed_connection_id, endpoint.clone()))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    // There is no one to return the error to, so just log it
                    tracing::error!("Failed to add ip to client manager: {:?}", e);
                }
            }
        }
    }

    fn indexed_connection_id(node: &str, index: usize) -> String {
        format!("{}-{}", node, index)
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
        // NOTE(hammadb) In production, we assume that each query service is 1:1 with a node. I.e that no
        // two query services are running on the same node. However, in local
        // development, we may have multiple query services running on the same node.
        // In order to handle this, we append the member_id to the node name to make it unique.
        // This is purely for local development purposes.

        // Determine if all members share a node
        let mut all_same_node = true;
        let mut node = "";
        for new_member in new_members.iter() {
            if node.is_empty() {
                node = new_member.member_node_name.as_str();
            } else if node != new_member.member_node_name.as_str() {
                all_same_node = false;
                break;
            }
        }

        // Rewrite the memberlist to include the member_id in the node name
        // if they all share the same node
        let mut rewritten_new_members = Vec::new();
        for new_member in new_members.iter() {
            let mut new_member = new_member.clone();
            if all_same_node {
                new_member.member_node_name =
                    format!("{}-{}", new_member.member_node_name, new_member.member_id);
            }
            rewritten_new_members.push(new_member);
        }
        let new_members = rewritten_new_members;

        // Process the new memberlist, determining if any nodes have been added or removed
        // or if any nodes have changed their ip address
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
