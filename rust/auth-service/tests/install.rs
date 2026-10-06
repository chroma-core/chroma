use std::sync::{Arc, Mutex};

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::IntoResponse,
    Json, Router,
};
use chroma_auth_service::{config::Config, install::install};
use http_body_util::BodyExt;
use serde_json::{json, Value};

#[derive(Default)]
struct Mock {
    tenant: bool,
    database: bool,
    fail_database: bool,
    reject: bool,
    requests: Vec<String>,
}

async fn handle(
    State(state): State<Arc<Mutex<Mock>>>,
    request: Request<Body>,
) -> impl IntoResponse {
    assert_eq!(
        request.headers()["x-chroma-token"],
        "example-tenant-key-replace-me"
    );
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let bytes = request.into_body().collect().await.unwrap().to_bytes();
    let mut state = state.lock().unwrap();
    state.requests.push(format!("{method} {path}"));
    let mut body = json!({});
    let status = if state.reject {
        StatusCode::UNAUTHORIZED
    } else if method == "POST" {
        let payload: Value = serde_json::from_slice(&bytes).unwrap();
        let exists = match path.as_str() {
            "/api/v2/tenants" => {
                assert_eq!(payload, json!({"name": "default_tenant"}));
                &mut state.tenant
            }
            "/api/v2/tenants/default_tenant/databases" => {
                assert!(state.tenant);
                assert_eq!(payload, json!({"name": "default_database"}));
                if state.fail_database {
                    return (StatusCode::SERVICE_UNAVAILABLE, Json(body));
                }
                &mut state.database
            }
            _ => panic!("unexpected route"),
        };
        if *exists {
            StatusCode::CONFLICT
        } else {
            *exists = true;
            StatusCode::OK
        }
    } else {
        body = match path.as_str() {
            "/api/v2/tenants/default_tenant" if state.tenant => json!({"name": "default_tenant"}),
            "/api/v2/tenants/default_tenant/databases/default_database" if state.database => {
                json!({"name": "default_database"})
            }
            _ => panic!("unexpected read"),
        };
        StatusCode::OK
    };
    (status, Json(body))
}

async fn server(state: Arc<Mutex<Mock>>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(handle).with_state(state))
            .await
            .unwrap();
    });
    (url, task)
}

#[tokio::test]
async fn installs_twice_and_recovers_from_partial_install() {
    let state = Arc::new(Mutex::new(Mock {
        fail_database: true,
        ..Default::default()
    }));
    let (url, task) = server(state.clone()).await;
    let config = Config::parse(include_str!(
        "../../../k8s/auth-service/config.example.toml"
    ))
    .unwrap();
    assert!(install(&config, &url).await.is_err());
    assert!(state.lock().unwrap().tenant);
    assert!(!state.lock().unwrap().database);
    state.lock().unwrap().fail_database = false;
    install(&config, &url).await.unwrap();
    install(&config, &url).await.unwrap();
    assert!(state.lock().unwrap().database);
    assert!(state
        .lock()
        .unwrap()
        .requests
        .contains(&"GET /api/v2/tenants/default_tenant/databases/default_database".to_owned()));
    task.abort();
}

#[tokio::test]
async fn authentication_failure_stops_before_creating_database() {
    let state = Arc::new(Mutex::new(Mock {
        reject: true,
        ..Default::default()
    }));
    let (url, task) = server(state.clone()).await;
    let config = Config::parse(include_str!(
        "../../../k8s/auth-service/config.example.toml"
    ))
    .unwrap();
    let error = install(&config, &url).await.unwrap_err().to_string();
    assert!(error.contains("401"));
    assert_eq!(state.lock().unwrap().requests.len(), 1);
    task.abort();
}

#[tokio::test]
async fn rejects_unsafe_endpoint_shapes() {
    let config = Config::parse(include_str!(
        "../../../k8s/auth-service/config.example.toml"
    ))
    .unwrap();
    for endpoint in [
        "file:///tmp/data",
        "http://user:password@localhost",
        "http://localhost/path",
        "http://localhost/?query=yes",
    ] {
        assert!(install(&config, endpoint).await.is_err());
    }
}
