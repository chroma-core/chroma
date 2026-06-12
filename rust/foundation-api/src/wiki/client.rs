//! Chroma client used by wiki routes to proxy record I/O to the FE.
//!
//! A single long-lived [`ChromaHttpClient`] (one shared connection pool to the
//! FE ingress) is built once at startup. Each request cheaply re-scopes it via
//! [`ChromaHttpClient::with_scope`], which swaps the caller's auth method
//! (forwarding their `x-chroma-token`), tenant, and the `FOUNDATION` database
//! in one call, so the FE remains the single point that enforces auth, quota,
//! metering, and billing while connections stay pooled across requests and
//! tenants.
//!
//! Requests target the FE's HAProxy ingress URL (not the internal ClusterIP)
//! so the ingress can consistent-hash on the collection id in the request
//! path. The one call the ingress cannot hash to the right replica is the
//! initial get-collection-by-name lookup, so the resolved wiki collection
//! identity (id + schema + metadata) is cached per tenant and used to rebuild
//! a handle on subsequent requests.

use chroma::client::{ChromaAuthMethod, ChromaHttpClientError, ChromaHttpClientOptions};
use chroma::{ChromaCollection, ChromaHttpClient};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::Collection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::FoundationConfig;

/// How long a resolved wiki collection identity is reused before it is
/// re-fetched from the FE. Short enough that schema/metadata edits propagate
/// quickly, long enough to keep the unhashable name lookup off the hot path.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);

/// Errors raised while resolving or building the proxying Chroma client.
#[derive(Debug, thiserror::Error)]
pub enum WikiClientError {
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

impl ChromaError for WikiClientError {
    fn code(&self) -> ErrorCodes {
        match self {
            WikiClientError::MissingIngressUrl | WikiClientError::InvalidIngressUrl { .. } => {
                ErrorCodes::Internal
            }
            WikiClientError::InvalidToken(_) => ErrorCodes::InvalidArgument,
            WikiClientError::Client(_) => ErrorCodes::Internal,
        }
    }
}

/// A cheaply-cloneable factory for per-request Chroma collection handles that
/// proxy to the FE. Holds one long-lived client (shared connection pool) plus
/// a tenant-scoped cache of the wiki collection identity.
#[derive(Clone, Debug)]
pub struct WikiClient {
    base_client: ChromaHttpClient,
    database_name: String,
    wiki_collection_name: String,
    cache: Arc<WikiCollectionCache>,
}

impl WikiClient {
    /// Builds a [`WikiClient`] from foundation config.
    ///
    /// Returns [`WikiClientError::MissingIngressUrl`] when no ingress URL is
    /// configured (the caller should treat the route as disabled), or
    /// [`WikiClientError::InvalidIngressUrl`] when it cannot be parsed.
    pub fn from_config(foundation: &FoundationConfig) -> Result<Self, WikiClientError> {
        let ingress_url = foundation
            .frontend_ingress_url
            .as_ref()
            .ok_or(WikiClientError::MissingIngressUrl)?;
        let endpoint: reqwest::Url =
            ingress_url
                .parse()
                .map_err(|err| WikiClientError::InvalidIngressUrl {
                    url: ingress_url.clone(),
                    message: format!("{err}"),
                })?;
        // One process-wide client/connection pool, with no credential or tenant
        // of its own. Per request we re-scope it to the caller's token, tenant,
        // and the FOUNDATION database via `scoped_client`, which shares this
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
            database_name: foundation.database_name.clone(),
            wiki_collection_name: foundation.wiki_collection.clone(),
            cache: Arc::new(WikiCollectionCache::new(DEFAULT_CACHE_TTL)),
        })
    }

    /// Re-scopes the shared base client to the caller's token, `tenant`, and
    /// the FOUNDATION database in one shot, reusing the existing connection
    /// pool. Setting the tenant explicitly also avoids an identity lookup round
    /// trip.
    fn scoped_client(
        &self,
        tenant: &str,
        token: &str,
    ) -> Result<ChromaHttpClient, WikiClientError> {
        let auth_method = ChromaAuthMethod::cloud_api_key(token)
            .map_err(|err| WikiClientError::InvalidToken(err.to_string()))?;
        Ok(self
            .base_client
            .with_scope(auth_method, tenant, &self.database_name))
    }

    /// Resolves a handle to the foundation `wiki` collection for this request,
    /// reusing the cached collection identity when available so the
    /// unhashable get-collection-by-name lookup stays off the hot path.
    pub async fn wiki_collection(
        &self,
        tenant: &str,
        token: &str,
    ) -> Result<ChromaCollection, WikiClientError> {
        let client = self.scoped_client(tenant, token)?;
        if let Some(collection) = self.cache.get(tenant) {
            return Ok(ChromaCollection::from_collection_model(client, collection));
        }
        let collection = client.get_collection(&self.wiki_collection_name).await?;
        self.cache
            .put(tenant.to_string(), collection.to_collection_model());
        Ok(collection)
    }

    /// Drops the cached wiki collection identity for `tenant`. Call this after
    /// a `NotFound` from the FE, since the collection may have been recreated
    /// with a new id.
    pub fn invalidate(&self, tenant: &str) {
        self.cache.invalidate(tenant);
    }
}

/// A tenant-keyed, TTL-bounded cache of resolved wiki collection identities.
#[derive(Debug)]
struct WikiCollectionCache {
    ttl: Duration,
    entries: Mutex<HashMap<String, CacheEntry>>,
}

#[derive(Debug)]
struct CacheEntry {
    collection: Collection,
    inserted_at: Instant,
}

impl WikiCollectionCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn get(&self, tenant: &str) -> Option<Collection> {
        self.get_at(tenant, Instant::now())
    }

    fn get_at(&self, tenant: &str, now: Instant) -> Option<Collection> {
        let mut entries = self.lock();
        match entries.get(tenant) {
            Some(entry) if now.duration_since(entry.inserted_at) < self.ttl => {
                Some(entry.collection.clone())
            }
            Some(_) => {
                entries.remove(tenant);
                None
            }
            None => None,
        }
    }

    fn put(&self, tenant: String, collection: Collection) {
        self.put_at(tenant, collection, Instant::now());
    }

    fn put_at(&self, tenant: String, collection: Collection, now: Instant) {
        self.lock().insert(
            tenant,
            CacheEntry {
                collection,
                inserted_at: now,
            },
        );
    }

    fn invalidate(&self, tenant: &str) {
        self.lock().remove(tenant);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, CacheEntry>> {
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

    fn collection_named(name: &str) -> Collection {
        Collection {
            name: name.to_string(),
            tenant: "tenant".to_string(),
            database: "FOUNDATION".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn cache_returns_entry_within_ttl() {
        let cache = WikiCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        let collection = collection_named("wiki");
        let id = collection.collection_id;

        cache.put_at("t1".to_string(), collection, start);

        let hit = cache
            .get_at("t1", start + Duration::from_secs(299))
            .expect("entry should be live within ttl");
        assert_eq!(hit.collection_id, id);
        assert_eq!(hit.name, "wiki");
    }

    #[test]
    fn cache_expires_entry_after_ttl() {
        let cache = WikiCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        cache.put_at("t1".to_string(), collection_named("wiki"), start);

        assert!(cache
            .get_at("t1", start + Duration::from_secs(300))
            .is_none());
        // Expired entries are evicted, not just hidden.
        assert!(cache.entries.lock().unwrap().is_empty());
    }

    #[test]
    fn cache_is_keyed_per_tenant() {
        let cache = WikiCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        cache.put_at("t1".to_string(), collection_named("wiki"), start);

        assert!(cache.get_at("t1", start).is_some());
        assert!(cache.get_at("t2", start).is_none());
    }

    #[test]
    fn invalidate_drops_cached_entry() {
        let cache = WikiCollectionCache::new(Duration::from_secs(300));
        let start = Instant::now();
        cache.put_at("t1".to_string(), collection_named("wiki"), start);

        cache.invalidate("t1");
        assert!(cache.get_at("t1", start).is_none());
    }

    #[test]
    fn from_config_requires_ingress_url() {
        let config = FoundationConfig::default();
        let err = WikiClient::from_config(&config).unwrap_err();
        assert!(matches!(err, WikiClientError::MissingIngressUrl));
    }

    #[test]
    fn from_config_rejects_invalid_ingress_url() {
        let config = FoundationConfig {
            frontend_ingress_url: Some("not a url".to_string()),
            ..FoundationConfig::default()
        };
        let err = WikiClient::from_config(&config).unwrap_err();
        assert!(matches!(err, WikiClientError::InvalidIngressUrl { .. }));
    }

    #[test]
    fn from_config_accepts_valid_ingress_url() {
        let config = FoundationConfig {
            frontend_ingress_url: Some("https://foundation-fe.internal".to_string()),
            ..FoundationConfig::default()
        };
        let client = WikiClient::from_config(&config).expect("valid url");
        assert_eq!(client.database_name, "FOUNDATION");
        assert_eq!(client.wiki_collection_name, "wiki");
    }
}
