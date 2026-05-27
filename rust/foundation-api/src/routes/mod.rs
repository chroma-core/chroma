use crate::server::FoundationApiServer;
use axum::{routing::post, Router};

pub(crate) mod init;

pub(crate) fn router() -> Router<FoundationApiServer> {
    Router::new().route("/api/init", post(init::foundation_init))
}
