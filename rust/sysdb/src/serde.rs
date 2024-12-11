use serde::{Deserialize, Serialize};

// Serde types for sysdb on s3
#[derive(Serialize, Deserialize)]
pub(crate) struct TenantData {
    name: String,
}

impl TenantData {
    pub(crate) fn new(name: String) -> Self {
        TenantData { name }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct DatabaseData {
    name: String,
    id: String, // TODO: UUID?
}
