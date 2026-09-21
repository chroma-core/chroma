//! Product-owned Foundation identities and their backing Chroma databases.
//!
//! Implementations authorize every operation using the original caller. A
//! service credential identifies the registrar; it never expands caller access.
//! Implementations must reserve identities atomically and never rebind a name
//! to a different database during a retry. No registry is stored in sysdb.

use async_trait::async_trait;
use axum::http::HeaderMap;
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FoundationState {
    Provisioning,
    Ready,
    Unavailable,
    Deleted,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FoundationRecord {
    pub id: Uuid,
    pub tenant: String,
    pub name: String,
    pub database_id: Uuid,
    pub state: FoundationState,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReserveFoundation {
    pub tenant: String,
    pub name: String,
    /// Only default initialization may adopt an existing database, after a
    /// caller-authorized lookup. Named creation leaves this absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_id: Option<Uuid>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FoundationPage {
    pub foundations: Vec<FoundationRecord>,
    pub next_offset: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("Foundation catalog is not configured")]
    Unconfigured,
    #[error("Foundation catalog authentication failed")]
    Unauthorized,
    #[error("Foundation catalog access denied")]
    Forbidden,
    #[error("Foundation does not exist")]
    NotFound,
    #[error("Foundation identity conflicts with existing storage")]
    Conflict,
    #[error("Foundation is not ready")]
    NotReady,
    #[error("invalid Foundation catalog request: {0}")]
    InvalidArgument(String),
    #[error("Foundation catalog is unavailable: {0}")]
    Unavailable(String),
}

impl ChromaError for RegistryError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::Unauthorized => ErrorCodes::Unauthenticated,
            Self::Forbidden => ErrorCodes::PermissionDenied,
            Self::NotFound => ErrorCodes::NotFound,
            Self::Conflict => ErrorCodes::AlreadyExists,
            Self::InvalidArgument(_) => ErrorCodes::InvalidArgument,
            Self::NotReady | Self::Unconfigured | Self::Unavailable(_) => ErrorCodes::Unavailable,
        }
    }
}

#[async_trait]
pub trait FoundationRegistry: Send + Sync {
    async fn reserve(
        &self,
        headers: &HeaderMap,
        request: ReserveFoundation,
    ) -> Result<FoundationRecord, RegistryError>;
    async fn get(
        &self,
        headers: &HeaderMap,
        tenant: &str,
        name: &str,
    ) -> Result<FoundationRecord, RegistryError>;
    /// Authorize and filter before pagination, including effective database
    /// scope and the Foundation view permission. Failures are never empty pages.
    async fn list(
        &self,
        headers: &HeaderMap,
        tenant: &str,
        limit: u32,
        offset: u32,
    ) -> Result<FoundationPage, RegistryError>;
    async fn mark_ready(
        &self,
        headers: &HeaderMap,
        tenant: &str,
        name: &str,
        id: Uuid,
        database_id: Uuid,
    ) -> Result<FoundationRecord, RegistryError>;
}

/// Standalone deployments must explicitly install a product registry adapter.
/// A collection's name never substitutes for a missing product catalog.
pub struct UnconfiguredRegistry;

#[async_trait]
impl FoundationRegistry for UnconfiguredRegistry {
    async fn reserve(
        &self,
        _: &HeaderMap,
        _: ReserveFoundation,
    ) -> Result<FoundationRecord, RegistryError> {
        Err(RegistryError::Unconfigured)
    }
    async fn get(
        &self,
        _: &HeaderMap,
        _: &str,
        _: &str,
    ) -> Result<FoundationRecord, RegistryError> {
        Err(RegistryError::Unconfigured)
    }
    async fn list(
        &self,
        _: &HeaderMap,
        _: &str,
        _: u32,
        _: u32,
    ) -> Result<FoundationPage, RegistryError> {
        Err(RegistryError::Unconfigured)
    }
    async fn mark_ready(
        &self,
        _: &HeaderMap,
        _: &str,
        _: &str,
        _: Uuid,
        _: Uuid,
    ) -> Result<FoundationRecord, RegistryError> {
        Err(RegistryError::Unconfigured)
    }
}
