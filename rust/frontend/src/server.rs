use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::api::ServerApi;

#[derive(Clone)]
struct FrontendServerInner<T: ServerApi + Clone + Send + Sync + 'static> {
    pub server_api: T,
}

#[derive(Clone)]
pub(crate) struct FrontendServer<T: ServerApi + Clone + Send + Sync + 'static> {
    inner: FrontendServerInner<T>,
}

impl<T: ServerApi + Clone + Send + Sync + 'static> FrontendServer<T> {
    pub fn new(server_api: T) -> FrontendServer<T> {
        FrontendServer {
            inner: FrontendServerInner { server_api },
        }
    }

    #[allow(dead_code)]
    pub async fn run(server: FrontendServer<T>) {
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

    async fn create_database(&mut self, tenant: String, database_name: String) {
        let request = chroma_types::CreateDatabaseRequest {
            tenant_id: tenant,
            database_name,
            database_id: Uuid::new_v4(),
        };
        let resp = self.inner.server_api.create_database(request).await;
        // TODO: Return the correct HTTP status code.
        if resp.success {
            println!("Database created successfully");
        } else {
            println!("Failed to create database");
        }
    }
}

////////////////////////// Method Handlers //////////////////////////
// These handlers simply proxy the call and the relevant inputs into
// the appropriate method on the `FrontendServer` struct.

// Dummy implementation for now
async fn root<T: ServerApi + Clone + Send + Sync + 'static>(
    State(server): State<FrontendServer<T>>,
) -> &'static str {
    server.root()
}

#[derive(Deserialize, Debug)]
struct CreateDatabase {
    name: String,
}

async fn create_database<T: ServerApi + Clone + Send + Sync + 'static>(
    Path(tenant): Path<String>,
    State(mut server): State<FrontendServer<T>>,
    Json(payload): Json<CreateDatabase>,
) {
    println!(
        "Creating database for tenant: {} and name: {:?}",
        tenant, payload
    );
    server.create_database(tenant, payload.name).await;
}
