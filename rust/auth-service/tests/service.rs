use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chroma_auth_service::{config::Config, router};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

const CONFIG: &str = include_str!("../../../k8s/auth-service/config.example.toml");
const KEY: &str = "example-tenant-key-replace-me";
const SERVICE_KEY: &str = "example-data-plane-key-replace-me";

async fn call(path: &str, key: Option<&str>, body: Value) -> (StatusCode, Vec<u8>) {
    let mut request = Request::post(path).header("content-type", "application/json");
    if let Some(key) = key {
        request = request.header("x-chroma-data-plane-api-key", key);
    }
    let response = router(Config::parse(CONFIG).unwrap())
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    (
        response.status(),
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
}

#[tokio::test]
async fn authenticates_and_scopes_every_key_to_one_tenant() {
    for version in ["v1", "v2"] {
        let (status, body) = call(
            &format!("/api/{version}/check_api_key"),
            Some(SERVICE_KEY),
            json!({
                "apiKey": KEY, "team": "other-tenant",
                "checkCollectionIsPublic": {"type": "id", "collectionId": "anything"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["team"], "default_tenant");
        assert_eq!(body["identity"], 0);
        assert_eq!(body["type"], "api_key");
        assert_eq!(body["collectionIsPublic"], false);
        assert_eq!(body["databaseHasPublicCollection"], false);
        let permissions = body["permissions"].as_array().unwrap();
        assert!(permissions.contains(
            &json!({"resourceType": "tenant", "action": "create_tenant", "database": null})
        ));
        assert!(permissions
            .contains(&json!({"resourceType": "collection", "action": "query", "database": null})));
        assert!(!permissions.iter().any(|p| p["resourceType"] == "system"));
    }
}

#[tokio::test]
async fn rejects_invalid_credentials_on_all_routes() {
    for path in ["/api/v1/check_api_key", "/api/v2/check_api_key"] {
        for (service_key, tenant_key) in [
            (None, KEY),
            (Some("wrong"), KEY),
            (Some(KEY), KEY),
            (Some(SERVICE_KEY), "wrong"),
            (Some(SERVICE_KEY), ""),
            (Some(SERVICE_KEY), SERVICE_KEY),
        ] {
            let (status, body) = call(path, service_key, json!({"apiKey": tenant_key})).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert!(!String::from_utf8_lossy(&body).contains(KEY));
        }
        assert_eq!(
            call(path, Some(SERVICE_KEY), json!({})).await.0,
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
}

#[test]
fn config_rejects_invalid_values_without_disclosing_keys() {
    for config in [
        CONFIG.replace(KEY, ""),
        CONFIG.replace(SERVICE_KEY, KEY),
        CONFIG.replace("default_tenant", "../another-tenant"),
        CONFIG.replace("api_keys = [", "api_keys = [broken"),
        CONFIG.replace(&format!("[\"{KEY}\"]"), "[]"),
    ] {
        let error = Config::parse(&config)
            .err()
            .expect("must reject config")
            .to_string();
        assert!(!error.contains(KEY));
        assert!(!error.contains(SERVICE_KEY));
    }
}

#[tokio::test]
async fn accepts_multiple_keys() {
    let config = CONFIG.replace(
        &format!("[\"{KEY}\"]"),
        &format!("[\"{KEY}\", \"second-key\"]"),
    );
    let app = router(Config::parse(&config).unwrap());
    for key in [KEY, "second-key"] {
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v2/check_api_key")
                    .header("content-type", "application/json")
                    .header("x-chroma-data-plane-api-key", SERVICE_KEY)
                    .body(Body::from(json!({"apiKey": key}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["team"], "default_tenant");
    }
}
