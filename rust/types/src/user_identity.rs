use std::collections::HashMap;

use pyo3::pyclass;

/// UserIdentity represents the identity of a user. In general, not all fields
/// will be populated, and the fields that are populated will depend on the
/// authentication provider.
/// # Fields
/// - user_id: The user's unique identifier.
/// - tenant: The tenant the user is associated with.
/// - databases: The databases the user has access to.
/// - attributes: Additional attributes about the user needed by the Auth Implementation
#[derive(Debug)]
#[pyclass]
pub struct UserIdentity {
    #[pyo3(get)]
    pub user_id: String,
    #[pyo3(get)]
    pub tenant: Option<String>,
    #[pyo3(get)]
    pub databases: Vec<String>,
    #[pyo3(get)]
    pub attributes: HashMap<String, String>,
}

impl UserIdentity {
    /// Create a new UserIdentity.
    pub fn new(
        user_id: String,
        tenant: Option<String>,
        databases: Vec<String>,
        attributes: HashMap<String, String>,
    ) -> Self {
        UserIdentity {
            user_id,
            tenant,
            databases,
            attributes,
        }
    }
}

impl Default for UserIdentity {
    fn default() -> Self {
        UserIdentity {
            user_id: "".to_string(),
            tenant: Some("default_tenant".to_string()),
            databases: vec!["default_database".to_string()],
            attributes: HashMap::new(),
        }
    }
}
