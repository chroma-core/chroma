use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct GetUserIdentityResponse {
    pub user_id: String,
    pub tenant: String,
    pub databases: Vec<String>,
}
