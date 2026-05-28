use axum::http::HeaderMap;
use chroma_api_types::GetUserIdentityResponse;

use crate::auth::{AuthError, AuthenticateAndAuthorize, AuthzAction, AuthzResource};

/// Resolve the caller's identity from the auth token, then check that the
/// caller is allowed to perform `action` against their tenant.
///
/// The two-step shape exists because the Cloud `authenticate_and_authorize`
/// impl enforces `resource.tenant == user_identity.tenant` (returns 403 on
/// mismatch, including `resource.tenant == None`). The Noop impl ignores
/// resource entirely, which is why a single-call handler that passed
/// `tenant: None` looked fine in tests but 403'd under Cloud auth.
pub(super) async fn whoami_and_authorize(
    auth: &dyn AuthenticateAndAuthorize,
    headers: &HeaderMap,
    action: AuthzAction,
) -> Result<GetUserIdentityResponse, AuthError> {
    let identity = auth.get_user_identity(headers).await?;
    auth.authenticate_and_authorize(
        headers,
        action,
        AuthzResource {
            tenant: Some(identity.tenant.clone()),
            database: None,
            collection: None,
        },
    )
    .await?;
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::future::{ready, Future};
    use std::pin::Pin;
    use std::sync::Mutex;

    /// Fake auth that returns a fixed identity and records the action /
    /// resource passed to `authenticate_and_authorize` so tests can
    /// assert on it.
    struct FakeAuth {
        user_id: String,
        tenant: String,
        captured_action: Mutex<Option<AuthzAction>>,
        captured_resource: Mutex<Option<AuthzResource>>,
    }

    impl FakeAuth {
        fn new(user_id: &str, tenant: &str) -> Self {
            Self {
                user_id: user_id.to_string(),
                tenant: tenant.to_string(),
                captured_action: Mutex::new(None),
                captured_resource: Mutex::new(None),
            }
        }

        fn identity(&self) -> GetUserIdentityResponse {
            GetUserIdentityResponse {
                user_id: self.user_id.clone(),
                tenant: self.tenant.clone(),
                databases: HashSet::new(),
            }
        }
    }

    impl AuthenticateAndAuthorize for FakeAuth {
        fn authenticate_and_authorize(
            &self,
            _headers: &HeaderMap,
            action: AuthzAction,
            resource: AuthzResource,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            *self.captured_action.lock().unwrap() = Some(action);
            *self.captured_resource.lock().unwrap() = Some(resource);
            let identity = self.identity();
            Box::pin(ready(Ok(identity)))
        }

        fn authenticate_and_authorize_collection(
            &self,
            _headers: &HeaderMap,
            _action: AuthzAction,
            _resource: AuthzResource,
            _collection: chroma_types::Collection,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            let identity = self.identity();
            Box::pin(ready(Ok(identity)))
        }

        fn get_user_identity(
            &self,
            _headers: &HeaderMap,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            let identity = self.identity();
            Box::pin(ready(Ok(identity)))
        }
    }

    #[tokio::test]
    async fn passes_resolved_tenant_to_authz_and_returns_full_identity() {
        let fake = FakeAuth::new("user_99", "team_abc");
        let headers = HeaderMap::new();

        let identity = whoami_and_authorize(&fake, &headers, AuthzAction::InitFoundation)
            .await
            .expect("auth should succeed");

        assert_eq!(identity.tenant, "team_abc");
        assert_eq!(identity.user_id, "user_99");

        let captured_action = fake
            .captured_action
            .lock()
            .unwrap()
            .expect("authenticate_and_authorize should have been called");
        assert_eq!(captured_action, AuthzAction::InitFoundation);

        let captured = fake
            .captured_resource
            .lock()
            .unwrap()
            .clone()
            .expect("authenticate_and_authorize should have been called");
        // Regression: before this fix the handler passed `tenant: None`,
        // which the Cloud authz impl always rejects with 403.
        assert_eq!(captured.tenant, Some("team_abc".to_string()));
        assert_eq!(captured.database, None);
        assert_eq!(captured.collection, None);
    }
}
