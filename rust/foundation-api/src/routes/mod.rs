use crate::server::FoundationApiServer;
use axum::Router;

/// Empty foundation route module. Handler tickets (#7442, #7507, #7508, #7509)
/// register `/api/foundation/{ask,recall,brief,init}` and future sync-domain
/// endpoints here.
pub(crate) fn router() -> Router<FoundationApiServer> {
    Router::new()
}
