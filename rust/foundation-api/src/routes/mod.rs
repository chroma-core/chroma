use crate::server::FoundationApiServer;
use axum::{routing::post, Router};

pub(crate) mod init;

pub(crate) fn router() -> Router<FoundationApiServer> {
    Router::new().route("/api/foundation/init", post(init::foundation_init))
}
