use crate::server::FoundationApiServer;
use axum::{routing::post, Router};

pub(crate) mod init;
pub(crate) mod upsert_page;
pub(super) mod whoami;

pub(crate) fn router() -> Router<FoundationApiServer> {
    Router::new()
        .route("/api/init", post(init::foundation_init))
        .route(
            "/api/upsert-page",
            post(upsert_page::foundation_upsert_page),
        )
}
