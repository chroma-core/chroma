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

#[derive(Clone)]
pub struct GetDatabaseRequest {
    pub tenant_id: String,
    pub database_name: String,
}

#[derive(Clone)]
pub struct GetDatabaseResponse {
    pub database_id: Uuid,
    pub database_name: String,
    pub tenant_id: String,
}

#[derive(Error, Debug)]
pub enum GetDatabaseError {
    #[error("Database not found")]
    NotFound,
    #[error("Server sent empty response")]
    ResponseEmpty,
    #[error("Failed to parse database id")]
    IdParsingError,
    #[error("Failed to get database: {0}")]
    FailedToGetDatabase(String),
}

impl ChromaError for GetDatabaseError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetDatabaseError::NotFound => ErrorCodes::NotFound,
            _ => ErrorCodes::Internal,
        }
    }
}
