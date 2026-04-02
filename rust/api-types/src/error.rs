use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ErrorIndexingStatus {
    pub op_indexing_progress: f32,
    pub num_unindexed_ops: u64,
    pub num_indexed_ops: u64,
    pub total_ops: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexing_status: Option<ErrorIndexingStatus>,
}
