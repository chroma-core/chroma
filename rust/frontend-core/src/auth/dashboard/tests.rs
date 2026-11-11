use super::*;
use chroma_auth_service::{config::Config, router};

async fn service() -> (DashboardAuth, tokio::task::JoinHandle<()>) {
    let config = Config::parse(
        r#"
        tenant = "tenant-one"
        database = "database-one"
        api_keys = ["tenant-key"]
        data_plane_api_key = "service-key"
    "#,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let auth = DashboardAuth::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        "service-key",
    )
    .unwrap();
    let task = tokio::spawn(async { axum::serve(listener, router(config)).await.unwrap() });
    (auth, task)
}

fn headers(key: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-chroma-token", key.parse().unwrap());
    headers
}

fn resource(tenant: &str) -> AuthzResource {
    AuthzResource {
        tenant: Some(tenant.into()),
        database: Some("database-one".into()),
        collection: None,
    }
}

#[tokio::test]
async fn authenticates_against_cutout_and_enforces_tenant_boundary() {
    let (auth, server) = service().await;
    let identity = auth
        .get_user_identity(&headers("tenant-key"))
        .await
        .unwrap();
    assert_eq!(identity.tenant, "tenant-one");
    auth.authenticate_and_authorize(
        &headers("tenant-key"),
        AuthzAction::CreateTenant,
        resource("tenant-one"),
    )
    .await
    .unwrap();
    auth.authenticate_and_authorize(
        &headers("tenant-key"),
        AuthzAction::Query,
        resource("tenant-one"),
    )
    .await
    .unwrap();
    let denied = auth
        .authenticate_and_authorize(
            &headers("tenant-key"),
            AuthzAction::Query,
            resource("tenant-two"),
        )
        .await
        .unwrap_err();
    assert_eq!(denied.0, StatusCode::FORBIDDEN);
    let denied = auth
        .authenticate_and_authorize(
            &headers("tenant-key"),
            AuthzAction::Reset,
            resource("tenant-one"),
        )
        .await
        .unwrap_err();
    assert_eq!(denied.0, StatusCode::FORBIDDEN);
    for headers in [HeaderMap::new(), headers("invalid-key")] {
        assert_eq!(
            auth.get_user_identity(&headers).await.unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
    }
    let mut duplicate = headers("tenant-key");
    duplicate.append("x-chroma-token", "tenant-key".parse().unwrap());
    assert_eq!(
        auth.get_user_identity(&duplicate).await.unwrap_err().0,
        StatusCode::UNAUTHORIZED
    );
    server.abort();
}

#[tokio::test]
async fn rejects_collection_from_another_tenant_or_database() {
    let (auth, server) = service().await;
    let mut collection = Collection {
        tenant: "tenant-one".into(),
        database: "database-one".into(),
        ..Default::default()
    };
    auth.authenticate_and_authorize_collection(
        &headers("tenant-key"),
        AuthzAction::Query,
        resource("tenant-one"),
        collection.clone(),
    )
    .await
    .unwrap();
    collection.tenant = "tenant-two".into();
    assert_eq!(
        auth.authenticate_and_authorize_collection(
            &headers("tenant-key"),
            AuthzAction::Query,
            resource("tenant-one"),
            collection.clone()
        )
        .await
        .unwrap_err()
        .0,
        StatusCode::FORBIDDEN
    );
    collection.tenant = "tenant-one".into();
    collection.database = "database-two".into();
    assert_eq!(
        auth.authenticate_and_authorize_collection(
            &headers("tenant-key"),
            AuthzAction::Query,
            resource("tenant-one"),
            collection
        )
        .await
        .unwrap_err()
        .0,
        StatusCode::FORBIDDEN
    );
    server.abort();
}

#[test]
fn honors_database_and_action_permissions() {
    let claims = Claims {
        identity: 1,
        team: "tenant-one".into(),
        permissions: vec![Permission {
            resource_type: "collection".into(),
            action: "get".into(),
            database: Some("database-one".into()),
        }],
    };
    claims
        .authorize(AuthzAction::Get, &resource("tenant-one"))
        .unwrap();
    assert!(claims
        .authorize(AuthzAction::Delete, &resource("tenant-one"))
        .is_err());
    let mut other_database = resource("tenant-one");
    other_database.database = Some("database-two".into());
    assert!(claims.authorize(AuthzAction::Get, &other_database).is_err());
    assert!(claims
        .authorize(
            AuthzAction::Get,
            &AuthzResource {
                tenant: None,
                database: None,
                collection: None
            }
        )
        .is_err());
}

#[tokio::test]
async fn upstream_failure_does_not_authorize() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let auth = DashboardAuth::new(
        &format!("http://{}", listener.local_addr().unwrap()),
        "service-key",
    )
    .unwrap();
    let task = tokio::spawn(async {
        axum::serve(
            listener,
            axum::Router::new().fallback(|| async { "invalid JSON" }),
        )
        .await
        .unwrap()
    });
    assert_eq!(
        auth.get_user_identity(&headers("tenant-key"))
            .await
            .unwrap_err()
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    task.abort();
}
