use serde::{Deserialize, Serialize};

/// A stable message clients can use to identify conditional write conflicts.
pub const CONDITIONAL_WRITE_CONFLICT_MESSAGE: &str = "conditional write conflict";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}
