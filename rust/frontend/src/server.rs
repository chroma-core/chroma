use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chroma_types::{CreateDatabaseError, CreateDatabaseRequest};
use serde::Deserialize;
use uuid::Uuid;

use crate::frontend::Frontend;

#[derive(Clone)]
pub(crate) struct FrontendServer {
    frontend: Frontend,
}

impl FrontendServer {
    pub fn new(frontend: Frontend) -> FrontendServer {
        FrontendServer { frontend }
    }

    #[allow(dead_code)]
    pub async fn run(server: FrontendServer) {
        let app = Router::new()
            // `GET /` goes to `root`
            .route("/", get(root))
            .route("/api/v2/tenants/:tenant/databases", post(create_database))
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

#[derive(Deserialize, Debug)]
struct CreateDatabasePayload {
    name: String,
}

async fn create_database(
    Path(tenant): Path<String>,
    State(mut server): State<FrontendServer>,
    Json(payload): Json<CreateDatabasePayload>,
) -> Result<(), StatusCode> {
    println!(
        "Creating database for tenant: {} and name: {:?}",
        tenant, payload
    );
    let create_database_request = CreateDatabaseRequest {
        database_id: Uuid::new_v4(),
        tenant_id: tenant,
        database_name: payload.name,
    };
    let res = server
        .frontend
        .create_database(create_database_request)
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(e) => match e {
            CreateDatabaseError::AlreadyExists => Err(StatusCode::CONFLICT),
            CreateDatabaseError::FailedToCreateDatabase(_) => {
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}
