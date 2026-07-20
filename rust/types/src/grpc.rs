#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GrpcConfig {
    #[serde(default = "GrpcConfig::default_max_encoding_message_size")]
    pub max_encoding_message_size: usize,
    #[serde(default = "GrpcConfig::default_max_decoding_message_size")]
    pub max_decoding_message_size: usize,
    #[serde(default = "GrpcConfig::default_max_concurrent_streams")]
    pub max_concurrent_streams: u32,
}

impl GrpcConfig {
    fn default_max_encoding_message_size() -> usize {
        // NOTE(rescrv):  Set to match the frontend.
        40 * 1024 * 1024
    }

    fn default_max_decoding_message_size() -> usize {
        // NOTE(rescrv):  Set to match the frontend.
        40 * 1024 * 1024
    }

    fn default_max_concurrent_streams() -> u32 {
        // NOTE(rescrv):  Set to match the default.  The RLS needs 1000
        100
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            max_encoding_message_size: Self::default_max_encoding_message_size(),
            max_decoding_message_size: Self::default_max_decoding_message_size(),
            max_concurrent_streams: Self::default_max_concurrent_streams(),
        }
    }
}
