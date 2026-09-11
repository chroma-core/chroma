//! Test doubles shared by the route tests.
//!
//! Authorization is the one thing every route does the same way and gets wrong
//! differently, so the recording stub below lives here rather than inside one
//! route's test module.
//!
//! The module allows dead code because each affordance serves the routes that
//! can enter the mode it models: a stub that refuses every token is meaningless
//! to a route with no token gate, and a key fenced to named databases is
//! meaningless to a route that never filters by reach. An accessor with no
//! caller therefore says that no route needs that mode, not that the double is
//! wrong.
#![allow(dead_code)]

use std::collections::HashSet;
use std::future::{ready, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::{HeaderMap, StatusCode};
use chroma_api_types::GetUserIdentityResponse;
use chroma_sysdb::SysDb;
use chroma_system::System;

use crate::auth::{AuthError, AuthenticateAndAuthorize, AuthzAction, AuthzResource};
use crate::config::FoundationApiConfig;
use crate::errors::ServerError;
use crate::server::FoundationApiServer;

/// Authorization stub that answers with a fixed identity and records every
/// authorization it was asked for, in order.
///
/// Invariants:
/// 1. Every `authenticate_and_authorize` call is recorded, refused ones
///    included, so a test can tell "asked and denied" from "never asked".
/// 2. Authorization calls and identity lookups count separately, so a test can
///    prove a route resolved a pair without an identity round trip. The
///    collection entry point counts under neither, because no route here
///    authorizes through it.
/// 3. Refusals are opt-in. [`FakeAuth::enforcing_tenant_match`] reproduces the
///    Cloud implementation's 403 on a resource tenant the key does not own, and
///    [`FakeAuth::refusing`] reproduces its refusal of a token it will not
///    accept. The no-op implementation enforces neither, so only a stub can
///    produce either refusal.
/// 4. A blanket refusal outranks the tenant check: a stub built by
///    [`FakeAuth::refusing`] answers with that status whatever the resource
///    names.
/// 5. `databases` is the set of database names the key's permissions name. It
///    is empty for a tenant-wide key, which is what a reachability filter reads
///    as "every Foundation".
pub(in crate::routes) struct FakeAuth {
    user_id: String,
    tenant: String,
    databases: HashSet<String>,
    enforce_tenant_match: bool,
    refuse: Option<StatusCode>,
    authorizations: Mutex<Vec<(AuthzAction, AuthzResource)>>,
    identity_calls: AtomicUsize,
}

impl FakeAuth {
    /// A stub that authorizes every call and reports `tenant` as the key's own.
    pub(in crate::routes) fn new(user_id: &str, tenant: &str) -> Self {
        Self {
            user_id: user_id.to_string(),
            tenant: tenant.to_string(),
            databases: HashSet::new(),
            enforce_tenant_match: false,
            refuse: None,
            authorizations: Mutex::new(Vec::new()),
            identity_calls: AtomicUsize::new(0),
        }
    }

    /// A stub that answers 403 for a resource tenant other than `tenant`.
    pub(in crate::routes) fn enforcing_tenant_match(user_id: &str, tenant: &str) -> Self {
        Self {
            enforce_tenant_match: true,
            ..Self::new(user_id, tenant)
        }
    }

    /// A stub that refuses every authorization call with `status`.
    pub(in crate::routes) fn refusing(status: StatusCode) -> Self {
        Self {
            refuse: Some(status),
            ..Self::new("user_99", "team_abc")
        }
    }

    /// Scopes the key to the named databases, the way a database-scoped
    /// permission does. A key built without this is tenant-wide.
    pub(in crate::routes) fn scoped_to_databases(self, databases: &[&str]) -> Self {
        Self {
            databases: databases.iter().map(|name| name.to_string()).collect(),
            ..self
        }
    }

    fn identity(&self) -> GetUserIdentityResponse {
        GetUserIdentityResponse {
            user_id: self.user_id.clone(),
            tenant: self.tenant.clone(),
            databases: self.databases.clone(),
        }
    }

    /// Every authorization asked for, in the order it was asked.
    pub(in crate::routes) fn authorizations(&self) -> Vec<(AuthzAction, AuthzResource)> {
        self.authorizations
            .lock()
            .expect("lock should not be poisoned")
            .clone()
    }

    pub(in crate::routes) fn identity_calls(&self) -> usize {
        self.identity_calls.load(Ordering::SeqCst)
    }

    pub(in crate::routes) fn authorize_calls(&self) -> usize {
        self.authorizations
            .lock()
            .expect("lock should not be poisoned")
            .len()
    }

    /// The action of the single authorization the route asked for. Panics when
    /// the route asked for none or for more than one, because a test that
    /// reaches for "the" action is asserting there was exactly one. A route
    /// that authorizes more than once is read through
    /// [`FakeAuth::authorizations`] instead.
    pub(in crate::routes) fn captured_action(&self) -> AuthzAction {
        let authorizations = self.authorizations();
        assert_eq!(
            authorizations.len(),
            1,
            "expected exactly one authorization, got {}",
            authorizations.len()
        );
        authorizations[0].0
    }

    /// The resource of the single authorization the route asked for. Panics
    /// under the same condition as [`FakeAuth::captured_action`].
    pub(in crate::routes) fn captured_resource(&self) -> AuthzResource {
        let authorizations = self.authorizations();
        assert_eq!(
            authorizations.len(),
            1,
            "expected exactly one authorization, got {}",
            authorizations.len()
        );
        authorizations[0].1.clone()
    }
}

impl AuthenticateAndAuthorize for FakeAuth {
    fn authenticate_and_authorize(
        &self,
        _headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        let refusal = self.refuse.or_else(|| {
            let foreign_tenant =
                self.enforce_tenant_match && resource.tenant.as_deref() != Some(&self.tenant);
            foreign_tenant.then_some(StatusCode::FORBIDDEN)
        });
        self.authorizations
            .lock()
            .expect("lock should not be poisoned")
            .push((action, resource));
        if let Some(status) = refusal {
            return Box::pin(ready(Err(AuthError(status))));
        }
        let identity = self.identity();
        Box::pin(ready(Ok(identity)))
    }

    fn authenticate_and_authorize_collection(
        &self,
        _headers: &HeaderMap,
        _action: AuthzAction,
        _resource: AuthzResource,
        _collection: chroma_types::Collection,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        let identity = self.identity();
        Box::pin(ready(Ok(identity)))
    }

    fn get_user_identity(
        &self,
        _headers: &HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        self.identity_calls.fetch_add(1, Ordering::SeqCst);
        let identity = self.identity();
        Box::pin(ready(Ok(identity)))
    }
}

/// A server wired to `auth` and `sysdb`, with the default Foundation
/// configuration. Routes that reach the frontend are disabled, because no
/// ingress URL is configured.
pub(in crate::routes) fn server_with(auth: Arc<FakeAuth>, sysdb: SysDb) -> FoundationApiServer {
    server_with_config(FoundationApiConfig::default(), auth, sysdb)
}

/// A server wired to `auth`, `sysdb` and an explicit configuration.
pub(in crate::routes) fn server_with_config(
    config: FoundationApiConfig,
    auth: Arc<FakeAuth>,
    sysdb: SysDb,
) -> FoundationApiServer {
    FoundationApiServer::new(config, auth, sysdb, vec![], System::new())
}

/// Unwraps a handler result, reporting the error when a test expected success.
///
/// `ServerError` carries no `Debug`, so `unwrap` and `expect` are unavailable
/// on a result that holds one.
pub(in crate::routes) fn expect_ok<T>(result: Result<T, ServerError>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{context}: {error}"),
    }
}
