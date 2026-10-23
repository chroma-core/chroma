use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct HeartbeatResponse {
    #[serde(rename = "nanosecond heartbeat")]
    #[cfg_attr(feature = "utoipa", schema(rename = "nanosecond heartbeat"))]
    pub nanosecond_heartbeat: u128,
}
