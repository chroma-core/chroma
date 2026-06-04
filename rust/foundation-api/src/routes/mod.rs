use crate::server::FoundationApiServer;
use axum::{
    routing::{get, post},
    Router,
};

pub(crate) mod init;
pub(crate) mod sync_status;
pub(super) mod whoami;

pub(crate) fn router() -> Router<FoundationApiServer> {
    Router::new()
        .route("/api/init", post(init::foundation_init))
        .route("/api/sync-status", get(sync_status::foundation_sync_status))
}
