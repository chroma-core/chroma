use uuid::Uuid;

#[derive(Clone)]
pub struct CreateDatabaseRequest {
    pub database_id: Uuid,
    pub tenant_id: String,
    pub database_name: String,
}

#[derive(Clone)]
pub struct CreateDatabaseResponse {
    pub success: bool,
}
