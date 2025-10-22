use chroma_memberlist::config::CustomResourceMemberlistProviderConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct GrpcLogConfig {
    #[serde(default = "GrpcLogConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "GrpcLogConfig::default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "GrpcLogConfig::default_max_encoding_message_size")]
    pub max_encoding_message_size: usize,
    #[serde(default = "GrpcLogConfig::default_max_decoding_message_size")]
    pub max_decoding_message_size: usize,
    #[serde(default = "GrpcLogConfig::default_memberlist_provider")]
    pub memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
    #[serde(default = "GrpcLogConfig::default_assignment")]
    pub assignment: chroma_config::assignment::config::AssignmentPolicyConfig,
    #[serde(default = "GrpcLogConfig::default_port")]
    pub port: u16,
}

impl GrpcLogConfig {
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
                memberlist_name: "rust-log-service-memberlist".to_string(),
                queue_size: 100,
            },
        )
    }

    fn default_assignment() -> chroma_config::assignment::config::AssignmentPolicyConfig {
        chroma_config::assignment::config::AssignmentPolicyConfig::RendezvousHashing(
            chroma_config::assignment::config::RendezvousHashingAssignmentPolicyConfig {
                hasher: chroma_config::assignment::config::HasherType::Murmur3,
            },
        )
    }

    fn default_port() -> u16 {
        50051
    }
}

impl Default for GrpcLogConfig {
    fn default() -> Self {
        GrpcLogConfig {
            connect_timeout_ms: GrpcLogConfig::default_connect_timeout_ms(),
            request_timeout_ms: GrpcLogConfig::default_request_timeout_ms(),
            max_encoding_message_size: GrpcLogConfig::default_max_encoding_message_size(),
            max_decoding_message_size: GrpcLogConfig::default_max_decoding_message_size(),
            memberlist_provider: GrpcLogConfig::default_memberlist_provider(),
            assignment: GrpcLogConfig::default_assignment(),
            port: GrpcLogConfig::default_port(),
        }
    }
}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct SqliteLogConfig {
    pub tenant_id: String,
    pub topic_namespace: String,
}

impl Default for SqliteLogConfig {
    fn default() -> Self {
        SqliteLogConfig {
            tenant_id: "default".to_string(),
            topic_namespace: "default".to_string(),
        }
    }
}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub enum LogConfig {
    #[serde(alias = "grpc")]
    Grpc(GrpcLogConfig),
    #[serde(alias = "sqlite")]
    Sqlite(SqliteLogConfig),
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig::Grpc(GrpcLogConfig::default())
    }
}
