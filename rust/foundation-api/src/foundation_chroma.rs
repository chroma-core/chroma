//! Chroma client used by Foundation routes to proxy record I/O to the FE.
//!
//! A single long-lived [`ChromaHttpClient`] (one shared connection pool to the
//! FE ingress) is built once at startup. Each request cheaply re-scopes it via
//! [`ChromaHttpClient::with_scope`], which swaps the caller's auth method
//! (forwarding their `x-chroma-token`), tenant, and the database holding the
//! request's Foundation in one call, so the FE remains the single point that
//! enforces auth, quota, metering, and billing while connections stay pooled
//! across requests and tenants.
//!
//! Requests target the FE's HAProxy ingress URL (not the internal ClusterIP)
//! so the ingress can consistent-hash on the collection id in the request
//! path. The one call the ingress cannot hash to the right replica is the
//! initial get-collection-by-name lookup, so resolved Foundation collection
//! identities (id + schema + metadata) are cached per tenant, database and
//! collection name, then used to rebuild handles on subsequent requests.

use chroma::client::{ChromaAuthMethod, ChromaHttpClientError, ChromaHttpClientOptions};
use chroma::{ChromaCollection, ChromaHttpClient};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::Collection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::FoundationConfig;

/// How long a resolved Foundation collection identity is reused before it is
/// re-fetched from the FE. Short enough that schema/metadata edits propagate
/// quickly, long enough to keep the unhashable name lookup off the hot path.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);

/// Errors raised while resolving or building the proxying Chroma client.
#[derive(Debug, thiserror::Error)]
pub enum FoundationChromaClientError {
    /// `frontend_ingress_url` was not set in config, so the route is disabled.
    #[error("foundation frontend_ingress_url is not configured")]
    MissingIngressUrl,
    /// `frontend_ingress_url` was set but is not a valid URL.
    #[error("invalid frontend_ingress_url '{url}': {message}")]
    InvalidIngressUrl {
        /// The offending configured value.
        url: String,
        /// The underlying parse error rendered as a string.
        message: String,
    },
    /// The caller's `x-chroma-token` is not a valid HTTP header value.
    #[error("invalid x-chroma-token header value: {0}")]
    InvalidToken(String),
    /// The downstream Chroma client/FE returned an error.
    #[error("chroma client error: {0}")]
    Client(#[from] ChromaHttpClientError),
}

impl ChromaError for FoundationChromaClientError {
    fn code(&self) -> ErrorCodes {
        match self {
            FoundationChromaClientError::MissingIngressUrl
            | FoundationChromaClientError::InvalidIngressUrl { .. } => ErrorCodes::Internal,
            FoundationChromaClientError::InvalidToken(_) => ErrorCodes::InvalidArgument,
            FoundationChromaClientError::Client(_) => ErrorCodes::Internal,
        }
    }
}

impl FoundationChromaClientError {
    /// Whether this is a 404 from the FE — i.e. the addressed database or
    /// requested collection does not exist, so that Foundation isn't
    /// provisioned for this tenant (as opposed to a transient or internal
    /// failure).
    pub(crate) fn is_not_found(&self) -> bool {
        matches!(self, FoundationChromaClientError::Client(err) if is_not_found(err))
    }
}

/// A cheaply-cloneable factory for per-request Chroma collection handles that
/// proxy to the FE. Holds one long-lived client (shared connection pool) plus
/// a tenant/database/collection-scoped cache of Foundation collection
/// identities.
#[derive(Clone, Debug)]
pub struct FoundationChromaClient {
    base_client: ChromaHttpClient,
    wiki_collection_name: String,
    trajectories_collection_name: String,
    cache: Arc<FoundationCollectionCache>,
}

impl FoundationChromaClient {
    /// Builds a [`FoundationChromaClient`] from foundation config.
    ///
    /// Returns [`FoundationChromaClientError::MissingIngressUrl`] when no ingress URL is
    /// configured (the caller should treat the route as disabled), or
    /// [`FoundationChromaClientError::InvalidIngressUrl`] when it cannot be parsed.
    pub fn from_config(foundation: &FoundationConfig) -> Result<Self, FoundationChromaClientError> {
        let ingress_url = foundation
            .frontend_ingress_url
            .as_ref()
            .ok_or(FoundationChromaClientError::MissingIngressUrl)?;
        let endpoint: reqwest::Url =
            ingress_url
                .parse()
                .map_err(|err| FoundationChromaClientError::InvalidIngressUrl {
                    url: ingress_url.clone(),
                    message: format!("{err}"),
                })?;
        // One process-wide client/connection pool, with no credential or tenant
        // of its own. Per request we re-scope it to the caller's token, tenant,
        // and the request's database via `scoped_client`, which shares this
        // pool rather than opening a new one.
        let base_client = ChromaHttpClient::new(ChromaHttpClientOptions {
            endpoint,
            auth_method: ChromaAuthMethod::None,
            tenant_id: None,
            database_name: None,
            ..Default::default()
        });
        Ok(Self {
            base_client,
            wiki_collection_name: foundation.wiki_collection.clone(),
            trajectories_collection_name: foundation.trajectories_collection.clone(),
            cache: Arc::new(FoundationCollectionCache::new(DEFAULT_CACHE_TTL)),
        })
    }

    /// Re-scopes the shared base client to the caller's token, `tenant`, and
    /// `database` in one shot, reusing the existing connection pool. Setting
    /// the tenant explicitly also avoids an identity lookup round trip.
    ///
    /// The database is a parameter rather than a field so that one process can
    /// serve every Foundation in a tenant.
    fn scoped_client(
        &self,
        tenant: &str,
        database: &str,
        token: &str,
    ) -> Result<ChromaHttpClient, FoundationChromaClientError> {
        let auth_method = ChromaAuthMethod::cloud_api_key(token)
            .map_err(|err| FoundationChromaClientError::InvalidToken(err.to_string()))?;
        Ok(self.base_client.with_scope(auth_method, tenant, database))
    }

    /// Resolves a Foundation collection by configured name inside `database`,
    /// reusing the cached collection identity when available so collection-id
    /// routes stay hot.
    ///
    /// Invariants:
    /// 1. A cached entry is served only when its own tenant and database match
    ///    the request. The Chroma client builds its data-plane URL from the
    ///    cached collection model, so handing back an entry that belongs
    ///    elsewhere would read and write the wrong Foundation.
    /// 2. Only a model that would pass that check is cached. A model the check
    ///    rejects can never be served, so storing it costs every later request
    ///    a warning, an invalidation and a fresh resolve while the entry is
    ///    written back unchanged.
    pub async fn collection(
        &self,
        tenant: &str,
        database: &str,
        token: &str,
        collection_name: &str,
    ) -> Result<ChromaCollection, FoundationChromaClientError> {
        let client = self.scoped_client(tenant, database, token)?;
        if let Some(collection) = self.cache.get(tenant, database, collection_name) {
            if belongs_to(&collection, tenant, database) {
                return Ok(ChromaCollection::from_collection_model(client, collection));
            }
            tracing::warn!(
                requested_tenant = %tenant,
                requested_database = %database,
                cached_tenant = %collection.tenant,
                cached_database = %collection.database,
                collection = %collection_name,
                "cached foundation collection belongs to another tenant or database; re-resolving"
            );
            self.cache.invalidate(tenant, database, collection_name);
        }
        let collection = client.get_collection(collection_name).await?;
        let resolved = collection.to_collection_model();
        if belongs_to(&resolved, tenant, database) {
            self.cache.put(
                tenant.to_string(),
                database.to_string(),
                collection_name.to_string(),
                resolved,
            );
        } else {
            tracing::warn!(
                requested_tenant = %tenant,
                requested_database = %database,
                resolved_tenant = %resolved.tenant,
                resolved_database = %resolved.database,
                collection = %collection_name,
                "resolved foundation collection belongs to another tenant or database; not caching it"
            );
        }
        Ok(collection)
    }

    /// Resolves a handle to the foundation `wiki` collection for this request,
    /// reusing the cached collection identity when available so the
    /// unhashable get-collection-by-name lookup stays off the hot path.
    pub async fn wiki_collection(
        &self,
        tenant: &str,
        database: &str,
        token: &str,
    ) -> Result<ChromaCollection, FoundationChromaClientError> {
        self.collection(tenant, database, token, &self.wiki_collection_name)
            .await
    }

    /// Resolves a handle to the generated-trajectory collection.
    pub async fn trajectories_collection(
        &self,
        tenant: &str,
        database: &str,
        token: &str,
    ) -> Result<ChromaCollection, FoundationChromaClientError> {
        self.collection(tenant, database, token, &self.trajectories_collection_name)
            .await
    }

    /// Drops one cached collection identity for `tenant` and `database`.
    ///
    /// Call this after a `NotFound` from the FE on a collection-id route, since
    /// that collection may have been recreated with a new id.
    pub fn invalidate(&self, tenant: &str, database: &str, collection_name: &str) {
        self.cache.invalidate(tenant, database, collection_name);
    }

    /// Drops the cached wiki collection identity for `tenant` and `database`.
    pub fn invalidate_wiki(&self, tenant: &str, database: &str) {
        self.invalidate(tenant, database, &self.wiki_collection_name);
    }

    /// Drops the cached trajectory collection identity for `tenant` and
    /// `database`.
    pub fn invalidate_trajectories(&self, tenant: &str, database: &str) {
        self.invalidate(tenant, database, &self.trajectories_collection_name);
    }
}

/// Whether a collection model addresses the Foundation the request named.
///
/// The Chroma client builds its data-plane URL from the model's own tenant and
/// database rather than from the pair the caller asked for, so a model that
/// disagrees addresses a different Foundation than the request does.
fn belongs_to(collection: &Collection, tenant: &str, database: &str) -> bool {
    collection.tenant == tenant && collection.database == database
}

/// Whether a proxied call failed because the resource was not found (HTTP 404).
/// A 404 on a collection-id path means the cached id is stale (the collection
/// was recreated/forked), so callers invalidate the cache on it.
pub(crate) fn is_not_found(err: &ChromaHttpClientError) -> bool {
    matches!(
        err,
        ChromaHttpClientError::ApiError(_, status) if *status == reqwest::StatusCode::NOT_FOUND
    )
}

/// A tenant/database/collection-keyed, TTL-bounded cache of resolved collection
/// identities.
#[derive(Debug)]
struct FoundationCollectionCache {
    ttl: Duration,
    entries: Mutex<HashMap<CacheKey, CacheEntry>>,
}

/// The identity of one cached collection. The database is part of the key
/// because the same collection name exists in every Foundation a tenant owns.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct CacheKey {
    tenant: String,
    database: String,
    collection_name: String,
}

#[derive(Debug)]
struct CacheEntry {
    collection: Collection,
    inserted_at: Instant,
}

impl FoundationCollectionCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn get(&self, tenant: &str, database: &str, collection_name: &str) -> Option<Collection> {
        self.get_at(tenant, database, collection_name, Instant::now())
    }

    fn get_at(
        &self,
        tenant: &str,
        database: &str,
        collection_name: &str,
        now: Instant,
    ) -> Option<Collection> {
        let key = CacheKey {
            tenant: tenant.to_string(),
            database: database.to_string(),
            collection_name: collection_name.to_string(),
        };
        let mut entries = self.lock();
        match entries.get(&key) {
            Some(entry) if now.duration_since(entry.inserted_at) < self.ttl => {
                Some(entry.collection.clone())
            }
            Some(_) => {
                entries.remove(&key);
                None
            }
            None => None,
        }
    }

    fn put(
        &self,
        tenant: String,
        database: String,
        collection_name: String,
        collection: Collection,
    ) {
        self.put_at(
            tenant,
            database,
            collection_name,
            collection,
            Instant::now(),
        );
    }

    fn put_at(
        &self,
        tenant: String,
        database: String,
        collection_name: String,
        collection: Collection,
        now: Instant,
    ) {
        self.lock().insert(
            CacheKey {
                tenant,
                database,
                collection_name,
            },
            CacheEntry {
                collection,
                inserted_at: now,
            },
        );
    }

    fn invalidate(&self, tenant: &str, database: &str, collection_name: &str) {
        self.lock().remove(&CacheKey {
            tenant: tenant.to_string(),
            database: database.to_string(),
            collection_name: collection_name.to_string(),
        });
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<CacheKey, CacheEntry>> {
        // A poisoned lock means a prior holder panicked while mutating the map.
        // The cache is rebuildable from the FE, so recover the guard rather than
        // propagating the panic.
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::MockServer;

    const DB: &str = "FOUNDATION";

    fn collection_named(name: &str) -> Collection {
        collection_in(name, "tenant", DB)
    }

    fn collection_in(name: &str, tenant: &str, database: &str) -> Collection {
        Collection {
            name: name.to_string(),
            tenant: tenant.to_string(),
            database: database.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn cache_returns_entry_within_ttl() {
        let cache = FoundationCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        let collection = collection_named("wiki");
        let id = collection.collection_id;

        cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            collection,
            start,
        );

        let hit = cache
            .get_at("t1", DB, "wiki", start + Duration::from_secs(299))
            .expect("entry should be live within ttl");
        assert_eq!(hit.collection_id, id);
        assert_eq!(hit.name, "wiki");
    }

    #[test]
    fn cache_expires_entry_after_ttl() {
        let cache = FoundationCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            collection_named("wiki"),
            start,
        );

        assert!(cache
            .get_at("t1", DB, "wiki", start + Duration::from_secs(300))
            .is_none());
        // Expired entries are evicted, not just hidden.
        assert!(cache.entries.lock().unwrap().is_empty());
    }

    #[test]
    fn cache_is_keyed_per_tenant() {
        let cache = FoundationCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            collection_named("wiki"),
            start,
        );

        assert!(cache.get_at("t1", DB, "wiki", start).is_some());
        assert!(cache.get_at("t2", DB, "wiki", start).is_none());
    }

    #[test]
    fn cache_is_keyed_per_database() {
        // The same collection name exists in every Foundation a tenant owns, so
        // the database has to be part of the key or one Foundation's handle
        // would serve another's reads and writes.
        let cache = FoundationCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            collection_named("wiki"),
            start,
        );

        assert!(cache.get_at("t1", DB, "wiki", start).is_some());
        assert!(cache
            .get_at("t1", "other_foundation", "wiki", start)
            .is_none());
    }

    #[test]
    fn cache_is_keyed_per_collection_name() {
        let cache = FoundationCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            collection_named("wiki"),
            start,
        );

        assert!(cache.get_at("t1", DB, "wiki", start).is_some());
        assert!(cache
            .get_at("t1", DB, "generate_trajectories", start)
            .is_none());
    }

    #[tokio::test]
    async fn a_cached_entry_from_another_database_is_treated_as_a_miss() {
        // A model whose own database disagrees with the request would send the
        // Chroma client at the wrong Foundation's data plane, so `collection`
        // drops it instead of returning it. The re-resolve then fails against
        // the unreachable test ingress, which is what proves the cached entry
        // was not used.
        let config = FoundationConfig {
            frontend_ingress_url: Some("http://127.0.0.1:1/".to_string()),
            ..FoundationConfig::default()
        };
        let client = FoundationChromaClient::from_config(&config).expect("valid url");
        client.cache.put(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            collection_in("wiki", "t1", "other_foundation"),
        );

        let err = client
            .collection("t1", DB, "ck-token", "wiki")
            .await
            .expect_err("a mismatched entry must not be served from cache");
        assert!(matches!(err, FoundationChromaClientError::Client(_)));
        // The mismatched entry is dropped rather than left to be served again.
        assert!(client.cache.get("t1", DB, "wiki").is_none());
    }

    #[tokio::test]
    async fn a_resolved_collection_from_another_database_is_not_cached() {
        // A model the served-from-cache check rejects is one this call would
        // never hand back, so caching it buys nothing and costs every later
        // request a warning, an invalidation and a fresh resolve that writes the
        // same unusable entry back.
        let mock_server = MockServer::start_async().await;
        let body = serde_json::to_value(collection_in("wiki", "t1", "other_foundation"))
            .expect("a collection should serialize");
        let resolve = mock_server
            .mock_async(move |when, then| {
                when.method("GET").path(format!(
                    "/api/v2/tenants/t1/databases/{DB}/collections/wiki"
                ));
                then.status(200).json_body(body.clone());
            })
            .await;
        let config = FoundationConfig {
            frontend_ingress_url: Some(mock_server.base_url()),
            ..FoundationConfig::default()
        };
        let client = FoundationChromaClient::from_config(&config).expect("valid url");

        client
            .collection("t1", DB, "ck-token", "wiki")
            .await
            .expect("the frontend answered, so the resolve succeeds");

        assert_eq!(resolve.calls(), 1);
        assert!(
            client.cache.get("t1", DB, "wiki").is_none(),
            "a model that could never be served must not be cached"
        );
    }

    #[test]
    fn invalidate_drops_cached_entry() {
        let cache = FoundationCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            collection_named("wiki"),
            start,
        );

        cache.invalidate("t1", DB, "wiki");
        assert!(cache.get_at("t1", DB, "wiki", start).is_none());
    }

    #[test]
    fn invalidate_drops_only_the_requested_collection_name() {
        let cache = FoundationCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        let wiki = collection_named("wiki");
        let trajectories = collection_named("generate_trajectories");
        cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            wiki.clone(),
            start,
        );
        cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "generate_trajectories".to_string(),
            trajectories,
            start,
        );

        cache.invalidate("t1", DB, "generate_trajectories");
        assert_eq!(cache.get_at("t1", DB, "wiki", start), Some(wiki));
        assert_eq!(cache.get_at("t1", DB, "generate_trajectories", start), None);
    }

    #[test]
    fn client_invalidates_wiki_and_trajectories_independently() {
        let config = FoundationConfig {
            frontend_ingress_url: Some("https://foundation-fe.internal".to_string()),
            ..FoundationConfig::default()
        };
        let client = FoundationChromaClient::from_config(&config).expect("valid url");
        let start = Instant::now();
        let wiki = collection_named("wiki");
        let trajectories = collection_named("generate_trajectories");
        client.cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            wiki.clone(),
            start,
        );
        client.cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "generate_trajectories".to_string(),
            trajectories.clone(),
            start,
        );

        client.invalidate_wiki("t1", DB);
        assert_eq!(client.cache.get_at("t1", DB, "wiki", start), None);
        assert_eq!(
            client
                .cache
                .get_at("t1", DB, "generate_trajectories", start),
            Some(trajectories.clone())
        );

        client.invalidate_trajectories("t1", DB);
        assert_eq!(client.cache.get_at("t1", DB, "wiki", start), None);
        assert_eq!(
            client
                .cache
                .get_at("t1", DB, "generate_trajectories", start),
            None
        );
    }

    #[test]
    fn client_invalidation_is_scoped_to_one_database() {
        let config = FoundationConfig {
            frontend_ingress_url: Some("https://foundation-fe.internal".to_string()),
            ..FoundationConfig::default()
        };
        let client = FoundationChromaClient::from_config(&config).expect("valid url");
        let start = Instant::now();
        let other = collection_in("wiki", "t1", "other_foundation");
        client.cache.put_at(
            "t1".to_string(),
            DB.to_string(),
            "wiki".to_string(),
            collection_named("wiki"),
            start,
        );
        client.cache.put_at(
            "t1".to_string(),
            "other_foundation".to_string(),
            "wiki".to_string(),
            other.clone(),
            start,
        );

        client.invalidate_wiki("t1", DB);
        assert_eq!(client.cache.get_at("t1", DB, "wiki", start), None);
        assert_eq!(
            client.cache.get_at("t1", "other_foundation", "wiki", start),
            Some(other)
        );
    }

    #[test]
    fn is_not_found_matches_only_http_404() {
        use reqwest::StatusCode;
        assert!(is_not_found(&ChromaHttpClientError::ApiError(
            "missing".to_string(),
            StatusCode::NOT_FOUND,
        )));
        assert!(!is_not_found(&ChromaHttpClientError::ApiError(
            "boom".to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )));
        assert!(!is_not_found(&ChromaHttpClientError::NoBackendAvailable));
    }

    #[test]
    fn foundation_chroma_client_error_is_not_found_only_on_client_404() {
        use reqwest::StatusCode;
        // A 404 from the FE (missing database / requested collection).
        assert!(
            FoundationChromaClientError::Client(ChromaHttpClientError::ApiError(
                "missing".to_string(),
                StatusCode::NOT_FOUND,
            ))
            .is_not_found()
        );
        // Other downstream failures are not "not provisioned".
        assert!(
            !FoundationChromaClientError::Client(ChromaHttpClientError::ApiError(
                "boom".to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
            .is_not_found()
        );
        assert!(!FoundationChromaClientError::MissingIngressUrl.is_not_found());
        assert!(!FoundationChromaClientError::InvalidToken("nope".to_string()).is_not_found());
    }

    #[test]
    fn from_config_requires_ingress_url() {
        let config = FoundationConfig::default();
        let err = FoundationChromaClient::from_config(&config).unwrap_err();
        assert!(matches!(
            err,
            FoundationChromaClientError::MissingIngressUrl
        ));
    }

    #[test]
    fn from_config_rejects_invalid_ingress_url() {
        let config = FoundationConfig {
            frontend_ingress_url: Some("not a url".to_string()),
            ..FoundationConfig::default()
        };
        let err = FoundationChromaClient::from_config(&config).unwrap_err();
        assert!(matches!(
            err,
            FoundationChromaClientError::InvalidIngressUrl { .. }
        ));
    }

    #[test]
    fn from_config_accepts_valid_ingress_url() {
        let config = FoundationConfig {
            frontend_ingress_url: Some("https://foundation-fe.internal".to_string()),
            ..FoundationConfig::default()
        };
        let client = FoundationChromaClient::from_config(&config).expect("valid url");
        assert_eq!(client.wiki_collection_name, "wiki");
        assert_eq!(client.trajectories_collection_name, "generate_trajectories");
    }
}
