use chroma_memberlist::config::CustomResourceMemberlistProviderConfig;
use serde::{Deserialize, Serialize};

/// Configuration for the gRPC heap service client.
///
/// This configures how to connect to the heap tender service instances via memberlist.
#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct GrpcHeapServiceConfig {
    /// Whether the heap service client is enabled. Defaults to false.
    #[serde(default = "GrpcHeapServiceConfig::default_enabled")]
    pub enabled: bool,
    /// Connection timeout in milliseconds. Defaults to 5000ms.
    #[serde(default = "GrpcHeapServiceConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Request timeout in milliseconds. Defaults to 5000ms.
    #[serde(default = "GrpcHeapServiceConfig::default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Maximum message size for encoding. Defaults to 32MB.
    #[serde(default = "GrpcHeapServiceConfig::default_max_encoding_message_size")]
    pub max_encoding_message_size: usize,
    /// Maximum message size for decoding. Defaults to 32MB.
    #[serde(default = "GrpcHeapServiceConfig::default_max_decoding_message_size")]
    pub max_decoding_message_size: usize,
    /// Memberlist provider configuration. Defaults to rust-log-service-memberlist (colocated).
    #[serde(default = "GrpcHeapServiceConfig::default_memberlist_provider")]
    pub memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
    /// Assignment policy. Must match log service (RendezvousHashing + Murmur3) for data locality.
    #[serde(default = "GrpcHeapServiceConfig::default_assignment")]
    pub assignment: chroma_config::assignment::config::AssignmentPolicyConfig,
    /// Port the heap service listens on. Defaults to 50052.
    #[serde(default = "GrpcHeapServiceConfig::default_port")]
    pub port: u16,
}

impl GrpcHeapServiceConfig {
    fn default_enabled() -> bool {
        false
    }

    fn default_connect_timeout_ms() -> u64 {
        5000
    }

    fn default_request_timeout_ms() -> u64 {
        5000
    }

    fn default_max_encoding_message_size() -> usize {
        32_000_000
    }

    fn default_max_decoding_message_size() -> usize {
        32_000_000
    }

    fn default_memberlist_provider() -> chroma_memberlist::config::MemberlistProviderConfig {
        chroma_memberlist::config::MemberlistProviderConfig::CustomResource(
            CustomResourceMemberlistProviderConfig {
                kube_namespace: "chroma".to_string(),
                memberlist_name: "rust-log-service-memberlist".to_string(), // Colocated with log service
                queue_size: 100,
            },
        )
    }

    fn default_assignment() -> chroma_config::assignment::config::AssignmentPolicyConfig {
        // IMPORTANT: Must match log service assignment policy (RendezvousHashing + Murmur3)
        // since heap and log services are colocated. This ensures that a collection's
        // heap operations go to the same node as its log operations.
        chroma_config::assignment::config::AssignmentPolicyConfig::RendezvousHashing(
            chroma_config::assignment::config::RendezvousHashingAssignmentPolicyConfig {
                hasher: chroma_config::assignment::config::HasherType::Murmur3,
            },
        )
    }

    fn default_port() -> u16 {
        50052
    }
}

impl Default for GrpcHeapServiceConfig {
    fn default() -> Self {
        GrpcHeapServiceConfig {
            enabled: GrpcHeapServiceConfig::default_enabled(),
            connect_timeout_ms: GrpcHeapServiceConfig::default_connect_timeout_ms(),
            request_timeout_ms: GrpcHeapServiceConfig::default_request_timeout_ms(),
            max_encoding_message_size: GrpcHeapServiceConfig::default_max_encoding_message_size(),
            max_decoding_message_size: GrpcHeapServiceConfig::default_max_decoding_message_size(),
            memberlist_provider: GrpcHeapServiceConfig::default_memberlist_provider(),
            assignment: GrpcHeapServiceConfig::default_assignment(),
            port: GrpcHeapServiceConfig::default_port(),
        }
    }
}

/// Configuration for heap service client.
#[derive(Deserialize, Clone, Serialize, Debug)]
pub enum HeapServiceConfig {
    /// gRPC-based heap service configuration.
    #[serde(alias = "grpc")]
    Grpc(GrpcHeapServiceConfig),
}

impl Default for HeapServiceConfig {
    fn default() -> Self {
        HeapServiceConfig::Grpc(GrpcHeapServiceConfig::default())
    }
}
