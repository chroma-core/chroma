use super::memberlist_provider::Memberlist;
use async_trait::async_trait;
use chroma_config::assignment::{
    assignment_policy::AssignmentPolicy, rendezvous_hash::AssignmentError,
};
use chroma_error::ChromaError;
use chroma_system::{Component, ComponentContext, Handler};
use chroma_tracing::GrpcClientTraceService;
use chroma_types::chroma_proto::{
    heap_tender_service_client::HeapTenderServiceClient, log_service_client::LogServiceClient,
    query_executor_client::QueryExecutorClient,
};
use parking_lot::RwLock;
use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};
use thiserror::Error;
use tonic::transport::{Channel, Endpoint};
use tower::{discover::Change, ServiceBuilder};

#[derive(Debug, Clone)]
pub struct ClientAssigner<T> {
    node_name_to_client: Arc<RwLock<HashMap<String, T>>>,
    assignment_policy: Box<dyn AssignmentPolicy>,
    replication_factor: usize,
}

#[derive(Error, Debug)]
pub enum ClientAssignmentError {
    #[error("No client found for node: {0}")]
    NoClientFound(String),
    #[error("Assignment error: {0}")]
    AssignmentError(#[from] AssignmentError),
}

impl ChromaError for ClientAssignmentError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            ClientAssignmentError::NoClientFound(_) => chroma_error::ErrorCodes::Internal,
            ClientAssignmentError::AssignmentError(_) => chroma_error::ErrorCodes::Internal,
        }
    }
}

impl<T> ClientAssigner<T>
where
    T: Clone,
{
    pub fn new(assignment_policy: Box<dyn AssignmentPolicy>, replication_factor: usize) -> Self {
        Self {
            node_name_to_client: Arc::new(RwLock::new(HashMap::new())),
            assignment_policy,
            replication_factor,
        }
    }

    /// Get the gRPC clients for the given key by performing the assignment policy
    /// # Arguments
    /// - `assignment_key` - The key for which the client is to be fetched
    /// # Returns
    /// - The gRPC clients for the given key in the order of the assignment policy with the target replication factor
    /// # Errors
    /// - If no client is found for the given key
    /// - If the assignment policy fails to assign the key
    pub fn clients(&mut self, assignment_key: &str) -> Result<Vec<T>, ClientAssignmentError> {
        self.assigned_clients(assignment_key)
            .map(|assigned_clients| assigned_clients.into_values().collect())
    }

    /// Get a map of assigned node names to their clients for the given key in a single lock acquisition
    /// # Arguments
    /// - `assignment_key` - The key for which the client is to be fetched
    /// # Returns
    /// - A HashMap<String, T> mapping assigned node names to their corresponding gRPC clients
    /// # Errors
    /// - If no client is found for the given key
    /// - If the assignment policy fails to assign the key
    pub fn assigned_clients(
        &mut self,
        assignment_key: &str,
    ) -> Result<HashMap<String, T>, ClientAssignmentError> {
        let node_name_to_client_guard = self.node_name_to_client.read();
        let members: Vec<String> = node_name_to_client_guard.keys().cloned().collect();
        let target_replication_factor = min(self.replication_factor, members.len());
        self.assignment_policy.set_members(members);
        let assigned = self
            .assignment_policy
            .assign(assignment_key, target_replication_factor)?;

        let assigned_clients = assigned
            .iter()
            .map(|node_name| {
                let client = node_name_to_client_guard
                    .get(node_name)
                    .ok_or_else(|| ClientAssignmentError::NoClientFound(node_name.clone()))?
                    .clone();
                Ok::<(String, T), ClientAssignmentError>((node_name.clone(), client))
            })
            .collect::<Result<HashMap<String, T>, ClientAssignmentError>>()?;
        Ok(assigned_clients)
    }

    /// Returns the names of all nodes currently managed by the assigner.
    pub fn node_names(&self) -> Vec<String> {
        self.node_name_to_client.read().keys().cloned().collect()
    }

    /// Returns the client for a specific node, if it exists.
    ///
    /// # Arguments
    ///
    /// * `node_name` - The name of the node to get the client for.
    pub fn client_for_node(&self, node_name: &str) -> Option<T> {
        self.node_name_to_client.read().get(node_name).cloned()
    }

    pub fn all(&self) -> Vec<T> {
        let node_name_to_client_guard = self.node_name_to_client.read();
        node_name_to_client_guard.values().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.node_name_to_client.read().is_empty()
    }
}

pub trait ClientFactory {
    fn new_from_channel(channel: GrpcClientTraceService<Channel>) -> Self;
    // TODO: Exposing/Proxy'ing each property manually like this is not ideal, if this bloats
    // we can consider better alternatives
    fn max_decoding_message_size(self, max_size: usize) -> Self;
}
#[derive(Debug)]
pub struct ClientOptions {
    max_response_size_bytes: Option<usize>,
}

impl ClientOptions {
    pub fn new(max_response_size_bytes: Option<usize>) -> Self {
        Self {
            max_response_size_bytes,
        }
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            max_response_size_bytes: Some(1024 * 1024 * 4), // 4 MB
        }
    }
}

/// A component that manages the gRPC clients for with a memberlist
/// # Fields
/// - `node_name_to_client` - A map from the node name to the gRPC client
/// - `node_name_to_change_sender` - A map from the node name to the sender to the channel to add / remove the ip
/// - `connections_per_node` - The number of connections to maintain per node
/// - `old_memberlist` - The old memberlist to compare against
/// # Notes
/// The client manager is responsible for creating and maintaining the gRPC clients for the query nodes.
/// It responds to changes to the memberlist and updates the clients accordingly.
#[derive(Debug)]
pub struct ClientManager<T> {
    client_assigner: ClientAssigner<T>,
    // The name of the node to the sender to the channel to add / remove the ip
    node_name_to_change_sender:
        HashMap<String, tokio::sync::mpsc::Sender<Change<String, Endpoint>>>,
    connections_per_node: usize,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
    port: u16,
    old_memberlist: Memberlist,
    options: ClientOptions,
}

impl<T> ClientManager<T>
where
    T: ClientFactory,
{
    pub fn new(
        client_assigner: ClientAssigner<T>,
        connections_per_node: usize,
        connect_timeout_ms: u64,
        request_timeout_ms: u64,
        port: u16,
        options: ClientOptions,
    ) -> Self {
        Self {
            client_assigner,
            node_name_to_change_sender: HashMap::new(),
            connections_per_node,
            connect_timeout_ms,
            request_timeout_ms,
            port,
            old_memberlist: Memberlist::new(),
            options,
        }
    }

    async fn remove_node(&mut self, node: &str) {
        let sender = match self.node_name_to_change_sender.get(node) {
            Some(sender) => sender,
            None => {
                // There is no one to return the error to, so just log it
                tracing::error!("Failed to find sender for node: {:?}", node);
                return;
            }
        };

        for i in 0..self.connections_per_node {
            let indexed_connection_id = Self::indexed_connection_id(node, i);
            let sender_clone = sender.clone();
            tokio::spawn(async move {
                match sender_clone
                    .send(Change::Remove(indexed_connection_id))
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        // There is no one to return the error to, so just log it
                        tracing::info!(
                            "Failed to remove ip from client manager: {:?}",
                            e.to_string()
                        );
                    }
                }
            });
        }

        let mut node_name_to_client_guard = self.client_assigner.node_name_to_client.write();
        node_name_to_client_guard.remove(node);
        self.node_name_to_change_sender.remove(node);
    }

    async fn add_ip_for_node(&mut self, ip: String, node: &str) {
        let ip_with_port = format!("http://{}:{}", ip, self.port);
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
                let (channel, channel_change_sender) =
                    Channel::balance_channel::<String>(self.connections_per_node);
                let channel = ServiceBuilder::new()
                    .layer(chroma_tracing::GrpcClientTraceLayer)
                    .service(channel);

                let client = T::new_from_channel(channel);
                let client = match self.options.max_response_size_bytes {
                    Some(max_size) => client.max_decoding_message_size(max_size),
                    None => client,
                };

                let mut node_name_to_client_guard =
                    self.client_assigner.node_name_to_client.write();
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
                    tracing::info!("Failed to add ip to client manager: {:?}", e);
                }
            }
        }
    }

    fn indexed_connection_id(node: &str, index: usize) -> String {
        format!("{}-{}", node, index)
    }

    async fn process_new_members(&mut self, new_members: Memberlist) {
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
                        // The ip has changed, remove the old node and add the new entry
                        self.remove_node(node).await;
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

        for (node, _) in old_node_to_ip.iter() {
            if !seen_nodes.contains(node) {
                // This node has been removed
                self.remove_node(node).await;
            }
        }

        self.old_memberlist = new_members;
    }
}

///////////////////////// Component Impl /////////////////////////

impl<T> Component for ClientManager<T>
where
    T: Debug + Send + Sync + 'static,
{
    fn get_name() -> &'static str {
        "ClientManger"
    }

    fn queue_size(&self) -> usize {
        1000
    }
}

#[async_trait]
impl<T> Handler<Memberlist> for ClientManager<T>
where
    T: ClientFactory + Debug + Send + Sync + 'static,
{
    type Result = ();

    async fn handle(&mut self, new_members: Memberlist, _ctx: &ComponentContext<ClientManager<T>>) {
        self.process_new_members(new_members).await;
    }
}

/////////////////////////// Client Factory Impls /////////////////////////

// Impl this trait on grpc client
impl ClientFactory
    for QueryExecutorClient<chroma_tracing::GrpcClientTraceService<tonic::transport::Channel>>
{
    fn new_from_channel(channel: GrpcClientTraceService<Channel>) -> Self {
        QueryExecutorClient::new(channel)
    }
    fn max_decoding_message_size(self, max_size: usize) -> Self {
        self.max_decoding_message_size(max_size)
    }
}

impl ClientFactory
    for LogServiceClient<chroma_tracing::GrpcClientTraceService<tonic::transport::Channel>>
{
    fn new_from_channel(channel: GrpcClientTraceService<Channel>) -> Self {
        LogServiceClient::new(channel)
    }
    fn max_decoding_message_size(self, max_size: usize) -> Self {
        self.max_decoding_message_size(max_size)
    }
}

impl ClientFactory
    for HeapTenderServiceClient<chroma_tracing::GrpcClientTraceService<tonic::transport::Channel>>
{
    fn new_from_channel(channel: GrpcClientTraceService<Channel>) -> Self {
        HeapTenderServiceClient::new(channel)
    }
    fn max_decoding_message_size(self, max_size: usize) -> Self {
        self.max_decoding_message_size(max_size)
    }
}

#[cfg(test)]
mod test {
    use super::super::memberlist_provider::Member;
    use super::*;
    use chroma_types::chroma_proto::query_executor_client::QueryExecutorClient;

    type QueryClient =
        QueryExecutorClient<chroma_tracing::GrpcClientTraceService<tonic::transport::Channel>>;

    fn test_client_manager() -> (ClientManager<QueryClient>, ClientAssigner<QueryClient>) {
        let client_assigner = ClientAssigner::new(
            Box::new(chroma_config::assignment::assignment_policy::RendezvousHashingAssignmentPolicy::default()),
            1,
        );
        let client_manager = ClientManager::new(
            client_assigner.clone(),
            1,
            1000,
            1000,
            50051,
            ClientOptions::default(),
        );
        (client_manager, client_assigner)
    }

    fn get_memberlist_of_size(size: usize) -> Memberlist {
        let mut memberlist = Memberlist::new();
        for i in 0..size {
            let member = Member {
                member_id: i.to_string(),
                member_node_name: format!("node{}", i),
                member_ip: format!("10.0.0.{}", i),
            };
            memberlist.push(member);
        }
        memberlist
    }

    #[tokio::test]
    async fn test_initialize_memberlist() {
        let (mut client_manager, client_assigner) = test_client_manager();
        let memberlist = get_memberlist_of_size(5);
        client_manager.process_new_members(memberlist.clone()).await;

        let node_name_to_client_guard = client_assigner.node_name_to_client.read();
        for member in memberlist.iter() {
            let node = member.member_node_name.as_str();
            node_name_to_client_guard
                .get(node)
                .expect("Client to exist");
        }
    }

    #[tokio::test]
    async fn test_add_new_node() {
        let (mut client_manager, client_assigner) = test_client_manager();
        let memberlist = get_memberlist_of_size(5);
        client_manager.process_new_members(memberlist.clone()).await;

        let memberlist_grown_by_one = get_memberlist_of_size(6);
        client_manager
            .process_new_members(memberlist_grown_by_one.clone())
            .await;

        let node_name_to_client_guard = client_assigner.node_name_to_client.read();
        for member in memberlist_grown_by_one.iter() {
            let node = member.member_node_name.as_str();
            node_name_to_client_guard
                .get(node)
                .expect("Client to exist");
        }
    }

    #[tokio::test]
    async fn test_remove_node_add_new() {
        let (mut client_manager, client_assigner) = test_client_manager();
        let memberlist = get_memberlist_of_size(5);
        client_manager.process_new_members(memberlist.clone()).await;

        let memberlist_shrunk_by_one = get_memberlist_of_size(4);
        // TODO: make this test work - right now the channel send hangs when
        // there is not connection in test environment
        client_manager
            .process_new_members(memberlist_shrunk_by_one.clone())
            .await;

        {
            let node_name_to_client_guard = client_assigner.node_name_to_client.read();
            for member in memberlist_shrunk_by_one.iter() {
                let node = member.member_node_name.as_str();
                node_name_to_client_guard
                    .get(node)
                    .expect("Client to exist");
            }

            let removed_node = memberlist.get(4).unwrap();
            let removed_node_name = removed_node.member_node_name.as_str();
            assert!(node_name_to_client_guard.get(removed_node_name).is_none());
        };

        let memberlist_grown_by_one = get_memberlist_of_size(5);
        client_manager
            .process_new_members(memberlist_grown_by_one.clone())
            .await;

        let node_name_to_client_guard = client_assigner.node_name_to_client.read();
        for member in memberlist_grown_by_one.iter() {
            let node = member.member_node_name.as_str();
            node_name_to_client_guard
                .get(node)
                .expect("Client to exist");
        }
    }

    // ClientAssigner tests
    #[tokio::test]
    async fn test_client_assigner_node_names_and_client_for_node() {
        let assigner: ClientAssigner<String> = ClientAssigner::new(
            Box::new(chroma_config::assignment::assignment_policy::RendezvousHashingAssignmentPolicy::default()),
            2,
        );
        {
            let mut guard = assigner.node_name_to_client.write();
            for i in 0..5 {
                guard.insert(format!("node{}", i), format!("client{}", i));
            }
        }
        let mut names = assigner.node_names();
        names.sort();
        assert_eq!(names, vec!["node0", "node1", "node2", "node3", "node4"]);

        assert_eq!(
            assigner.client_for_node("node3"),
            Some("client3".to_string())
        );
        assert!(assigner.client_for_node("missing").is_none());
    }
}
