//! Static single-tenant replacement for the dashboard's data-plane auth routes.

pub mod config;
pub mod install;

use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use config::Config;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

struct Service {
    key_hashes: Vec<[u8; 32]>,
    data_plane_hash: [u8; 32],
    identity: Value,
}

fn hash(key: &str) -> [u8; 32] {
    Sha256::digest(key.as_bytes()).into()
}

/// Build the dashboard-compatible routes from validated configuration.
/// Configuration is a startup snapshot; restart after updating the Secret.
pub fn router(config: Config) -> Router {
    let permissions: Vec<Value> = PERMISSIONS
        .iter()
        .flat_map(|(resource, actions)| {
            actions.iter().map(move |action| {
                json!({
                    "resourceType": resource, "action": action, "database": null
                })
            })
        })
        .collect();
    let state = Arc::new(Service {
        key_hashes: config.api_keys.iter().map(|k| hash(k)).collect(),
        data_plane_hash: hash(&config.data_plane_api_key),
        identity: json!({
            "identity": 0, "type": "api_key", "team": config.tenant,
            "permissions": permissions, "collectionIsPublic": false,
            "databaseHasPublicCollection": false
        }),
    });
    Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/api/v1/check_api_key", post(check_api_key))
        .route("/api/v2/check_api_key", post(check_api_key))
        .with_state(state)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KeyRequest {
    api_key: String,
}

impl Service {
    fn authenticate(&self, headers: &HeaderMap, request: &KeyRequest) -> Result<(), StatusCode> {
        let mut values = headers.get_all("x-chroma-data-plane-api-key").iter();
        let credential = values
            .next()
            .and_then(|v| v.to_str().ok())
            .ok_or(StatusCode::UNAUTHORIZED)?;
        if values.next().is_some() || !bool::from(hash(credential).ct_eq(&self.data_plane_hash)) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let candidate = hash(&request.api_key);
        let found = self
            .key_hashes
            .iter()
            .fold(subtle::Choice::from(0), |found, key| {
                found | key.ct_eq(&candidate)
            });
        if !bool::from(found) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        Ok(())
    }
}

async fn check_api_key(
    State(state): State<Arc<Service>>,
    headers: HeaderMap,
    Json(request): Json<KeyRequest>,
) -> Result<Json<Value>, StatusCode> {
    state.authenticate(&headers, &request)?;
    Ok(Json(state.identity.clone()))
}

// Explicit frontend-core AuthzAction wire grants. No system-wide reset grant.
const PERMISSIONS: &[(&str, &[&str])] = &[
    ("tenant", &["create_tenant", "get_tenant", "update_tenant"]),
    (
        "db",
        &[
            "create_database",
            "get_database",
            "delete_database",
            "list_databases",
            "list_collections",
            "count_collections",
            "create_collection",
            "get_or_create_collection",
        ],
    ),
    (
        "collection",
        &[
            "get_collection",
            "get_collection_by_crn",
            "update_collection",
            "delete_collection",
            "fork_collection",
            "count_forks",
            "add",
            "delete",
            "get",
            "query",
            "count",
            "update",
            "upsert",
            "search",
            "create_attached_function",
            "remove_attached_function",
        ],
    ),
];
