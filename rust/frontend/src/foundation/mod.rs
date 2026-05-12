use axum::Router;

use crate::server::FrontendServer;

pub(crate) fn router() -> Router<FrontendServer> {
    Router::new()
}
