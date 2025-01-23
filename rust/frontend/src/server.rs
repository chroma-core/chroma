use std::sync::Arc;

use axum::{extract::State, routing::get, Router};

struct FrontendServerInner {}

#[derive(Clone)]
pub(crate) struct FrontendServer {
    _inner: Arc<FrontendServerInner>,
}

impl FrontendServer {
    pub fn new() -> FrontendServer {
        FrontendServer {
            _inner: Arc::new(FrontendServerInner {}),
        }
    }

    #[allow(dead_code)]
    pub async fn run(server: FrontendServer) {
        let app = Router::new()
            // `GET /` goes to `root`
            .route("/", get(root))
            .with_state(server);

        // TODO: configuration for this
        // TODO: tracing
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }

    ////////////////////////// Method Implementations //////////////////////

    fn root(&self) -> &'static str {
        "Hello, World!"
    }
}

////////////////////////// Method Handlers //////////////////////////
// These handlers simply proxy the call and the relevant inputs into
// the appropriate method on the `FrontendServer` struct.

// Dummy implementation for now
async fn root(State(server): State<FrontendServer>) -> &'static str {
    server.root()
}
