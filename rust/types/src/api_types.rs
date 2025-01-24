use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone)]
pub struct CreateDatabaseRequest {
    pub database_id: Uuid,
    pub tenant_id: String,
    pub database_name: String,
}

#[derive(Clone)]
pub struct CreateDatabaseResponse {}

#[derive(Error, Debug)]
pub enum CreateDatabaseError {
    #[error("Database already exists")]
    AlreadyExists,
    #[error("Failed to create database: {0}")]
    FailedToCreateDatabase(String),
}

impl ChromaError for CreateDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateDatabaseError::AlreadyExists => ErrorCodes::AlreadyExists,
            CreateDatabaseError::FailedToCreateDatabase(_) => ErrorCodes::Internal,
        }
    }
}
