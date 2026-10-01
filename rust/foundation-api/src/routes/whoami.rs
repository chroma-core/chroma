use axum::http::{HeaderMap, StatusCode};
use chroma_api_types::GetUserIdentityResponse;
use chroma_error::{ChromaError, ErrorCodes};

use crate::auth::{AuthError, AuthenticateAndAuthorize, AuthzAction, AuthzResource};
use crate::routes::FoundationScope;

/// Longest Foundation name that can ever be deleted.
///
/// Soft-deleting a database rewrites its name as `_deleted_{name}_{uuid}`,
/// which adds 46 characters, into a `varchar(128)` column. A name that leaves
/// less than that much headroom makes the rewrite overflow the column, so the
/// delete rolls back on every attempt and the Foundation can never be removed.
const MAX_FOUNDATION_NAME_BYTES: usize = 128 - 46;

/// Product-reserved Foundation name. The explicit route hierarchy keeps the
/// resource name separate from this name-validation contract.
const RESERVED_FOUNDATION_NAME: &str = "foundations";

/// Why a request could not be resolved and authorized against one Foundation.
#[derive(Debug, thiserror::Error)]
pub(super) enum ScopeError {
    /// The path named a Foundation that is not a legal name.
    #[error("invalid foundation name '{name}': {message}")]
    InvalidFoundation { name: String, message: String },
    /// The path named a tenant that cannot be a tenant id.
    #[error("invalid tenant '{name}': {message}")]
    InvalidTenant { name: String, message: String },
    /// Authentication or authorization refused the request.
    #[error(transparent)]
    Auth(#[from] AuthError),
}

impl ChromaError for ScopeError {
    fn code(&self) -> ErrorCodes {
        match self {
            ScopeError::InvalidFoundation { .. } | ScopeError::InvalidTenant { .. } => {
                ErrorCodes::InvalidArgument
            }
            ScopeError::Auth(err) => err.code(),
        }
    }
}

/// Resolves the tenant and database from the path scope, then authorizes the
/// caller against exactly that pair in one call. Returns the resolved tenant,
/// the resolved database, and the caller's identity.
///
/// Invariants:
/// 1. Callers use the returned tenant and database, never the identity's
///    tenant, so a prefixed request cannot silently act on another Foundation.
/// 2. The authorization call enforces that the path tenant equals the key's tenant.
/// 3. The returned identity is the one the auth layer answered with, which on a
///    prefixed path is whatever the authorization call returned rather than a
///    separate identity lookup. Read `user_id` from it only where the two are
///    the same caller.
///
/// The resource passed to authorization names both the tenant and the database.
/// `tenant` must be `Some`: the Cloud `authenticate_and_authorize` impl enforces
/// `resource.tenant == user_identity.tenant` and answers 403 on a mismatch,
/// including `resource.tenant == None`. The Noop impl ignores the resource
/// entirely, which is why a handler that passed `tenant: None` looked fine in
/// tests but 403'd under Cloud auth.
///
/// `database` is `Some` so the resource names the Foundation the request
/// addresses. That decides nothing on its own: the Cloud impl accepts a
/// permission claim when the claim's database equals the resource's *or* the
/// claim names no database, and a Foundation permission claim names no database,
/// so both a named and an absent resource database are accepted. Naming it is
/// what makes the decision follow the request for a claim that does carry one.
/// The Foundation boundary itself is enforced downstream, by the frontend, which
/// re-checks the caller's data-plane claims on every proxied call.
pub(super) async fn authorize_scope(
    auth: &dyn AuthenticateAndAuthorize,
    headers: &HeaderMap,
    action: AuthzAction,
    scope: &FoundationScope,
) -> Result<(String, String, GetUserIdentityResponse), ScopeError> {
    validate_foundation_name(&scope.foundation).map_err(|message| {
        ScopeError::InvalidFoundation {
            name: scope.foundation.clone(),
            message,
        }
    })?;
    validate_path_tenant(&scope.tenant).map_err(|message| ScopeError::InvalidTenant {
        name: scope.tenant.clone(),
        message,
    })?;

    let tenant = scope.tenant.clone();
    let database = scope.foundation.clone();

    let authorized = auth
        .authenticate_and_authorize(
            headers,
            action,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: Some(database.clone()),
                collection: None,
            },
        )
        .await?;

    Ok((tenant, database, authorized))
}

/// Authorizes the operation and resolves its registered, ready product
/// identity before a memory route can reach its backing database.
pub(super) async fn authorize_registered_scope(
    server: &crate::server::FoundationApiServer,
    headers: &HeaderMap,
    action: AuthzAction,
    scope: &FoundationScope,
) -> Result<(String, String, GetUserIdentityResponse), crate::errors::ServerError> {
    let resolved = authorize_scope(&*server.auth, headers, action, scope).await?;
    super::foundations::require_ready_foundation(server, headers, &resolved.0, &resolved.1).await?;
    Ok(resolved)
}

/// Authenticates the caller, settles that the tenant in the path is the
/// caller's own, and answers with the caller's identity.
///
/// Invariants:
/// 1. A caller reaches only the tenant its key belongs to. A path naming any
///    other tenant is refused as forbidden rather than answered with an empty
///    result, so the shape of the answer tells a caller nothing about a tenant
///    it does not hold.
/// 2. This function checks tenant membership, not permission to read catalog
///    records. The caller must delegate that decision to the caller-authorized
///    product registry, which filters before pagination.
/// 3. The identity's `databases` field is a union across different action
///    grants. It is not an authorization filter for a particular operation.
pub(super) async fn authenticate_path_tenant(
    auth: &dyn AuthenticateAndAuthorize,
    headers: &HeaderMap,
    tenant: &str,
) -> Result<GetUserIdentityResponse, ScopeError> {
    validate_path_tenant(tenant).map_err(|message| ScopeError::InvalidTenant {
        name: tenant.to_string(),
        message,
    })?;

    let identity = auth.get_user_identity(headers).await?;
    if identity.tenant != tenant {
        return Err(ScopeError::Auth(AuthError(StatusCode::FORBIDDEN)));
    }
    Ok(identity)
}

/// Checks that `name` is a legal Foundation name.
///
/// A Foundation is a Chroma database and its name is the database name, so this
/// is [`chroma_types::validate_name`] plus three rules that database names alone
/// do not carry:
/// 1. At most [`MAX_FOUNDATION_NAME_BYTES`] bytes, so the Foundation stays
///    deletable.
/// 2. No `+`. The data plane reads a leading `topology+` as a multi-region
///    topology prefix, and `validate_name` deliberately accepts one `+`, so a
///    name carrying it addresses a different database than it spells.
/// 3. Not [`RESERVED_FOUNDATION_NAME`], which the CRUD routes occupy.
pub(super) fn validate_foundation_name(name: &str) -> Result<(), String> {
    if name.len() > MAX_FOUNDATION_NAME_BYTES {
        return Err(format!(
            "name must be at most {MAX_FOUNDATION_NAME_BYTES} bytes, got {}",
            name.len()
        ));
    }
    if name.contains('+') {
        return Err("name must not contain '+'".to_string());
    }
    if name == RESERVED_FOUNDATION_NAME {
        return Err(format!("'{RESERVED_FOUNDATION_NAME}' is a reserved name"));
    }
    chroma_types::validate_name(name).map_err(|err| {
        err.message
            .map(|message| message.to_string())
            .unwrap_or_else(|| "name is not a valid database name".to_string())
    })
}

/// Checks that `name` can be a tenant id.
///
/// Authorization is the real gate on the path tenant: it refuses any tenant the
/// caller's key does not own. This is the shape check underneath it, because the
/// tenant is interpolated into the data-plane URL without escaping, so a value
/// carrying a path separator would address a different route than it spells. A
/// deployment whose auth implementation enforces nothing would otherwise have no
/// guard at all.
///
/// The rule is an allow-list rather than a list of forbidden characters, because
/// the URL parser that builds the data-plane request rewrites the path before
/// sending it. It reads `\` as a separator and drops a segment of `.` or `..`,
/// so `x\..\victim` resolves to the tenant `victim`. It decodes `%2e` first, so
/// the text `%2e%2e` resolves the way `..` does, and a caller writing
/// `%252e%252e` reaches this check as exactly that text. Every one of those
/// spellings clears a list that forbids `/`, `?` and `#`.
///
/// Letters, digits, `.`, `_` and `-`, with both ends alphanumeric, leave the
/// parser nothing to rewrite: the only segments it drops are exactly `.` and
/// `..`, which the ends rule refuses, and every separator and escape it honours
/// falls outside the allowed set.
pub(super) fn validate_path_tenant(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("tenant must not be empty".to_string());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err("tenant must contain only letters, digits, '.', '_' and '-'".to_string());
    }
    let ends_are_alphanumeric = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric())
        && name
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_alphanumeric());
    if !ends_are_alphanumeric {
        return Err("tenant must start and end with a letter or digit".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::test_auth::FakeAuth;

    fn scope(tenant: &str, foundation: &str) -> FoundationScope {
        FoundationScope {
            tenant: tenant.to_string(),
            foundation: foundation.to_string(),
        }
    }

    #[tokio::test]
    async fn prefixed_path_uses_path_pair_without_an_identity_call() {
        let fake = FakeAuth::new("user_99", "team_abc");
        let headers = HeaderMap::new();

        let (tenant, database, identity) = authorize_scope(
            &fake,
            &headers,
            AuthzAction::ViewFoundation,
            &scope("team_abc", "wiki_team"),
        )
        .await
        .expect("auth should succeed");

        assert_eq!(tenant, "team_abc");
        assert_eq!(database, "wiki_team");
        assert_eq!(identity.user_id, "user_99");
        // The single authorization call also settles whether the key owns the
        // path tenant, so no separate identity lookup is made.
        assert_eq!(fake.identity_calls(), 0);
        assert_eq!(fake.authorize_calls(), 1);

        let captured = fake.captured_resource();
        assert_eq!(captured.tenant, Some("team_abc".to_string()));
        assert_eq!(captured.database, Some("wiki_team".to_string()));
    }

    #[tokio::test]
    async fn path_tenant_the_key_does_not_own_is_refused() {
        let fake = FakeAuth::enforcing_tenant_match("user_99", "team_abc");
        let headers = HeaderMap::new();

        let err = authorize_scope(
            &fake,
            &headers,
            AuthzAction::ViewFoundation,
            &scope("team_other", "wiki_team"),
        )
        .await
        .expect_err("a foreign tenant should be refused");

        assert_eq!(err.code(), ErrorCodes::PermissionDenied);
    }

    #[tokio::test]
    async fn a_path_tenant_the_key_owns_authenticates_and_answers_with_its_reach() {
        let fake = FakeAuth::new("user_99", "team_abc").scoped_to_databases(&["wiki_team"]);
        let headers = HeaderMap::new();

        let identity = authenticate_path_tenant(&fake, &headers, "team_abc")
            .await
            .expect("a caller's own tenant should authenticate");

        assert_eq!(identity.user_id, "user_99");
        // The reach comes back so the caller can ask about the databases the
        // key names, and no permission was checked to get it.
        assert!(identity.databases.contains("wiki_team"));
        assert_eq!(fake.identity_calls(), 1);
        assert_eq!(fake.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn a_path_tenant_the_key_does_not_own_is_refused() {
        let fake = FakeAuth::new("user_99", "team_abc");
        let headers = HeaderMap::new();

        let err = authenticate_path_tenant(&fake, &headers, "team_other")
            .await
            .expect_err("another tenant should be refused");

        // Forbidden rather than an empty answer: the shape of the answer must
        // tell a caller nothing about a tenant it does not hold.
        assert_eq!(err.code(), ErrorCodes::PermissionDenied);
    }

    #[tokio::test]
    async fn a_path_tenant_carrying_a_separator_is_refused_before_authenticating() {
        let fake = FakeAuth::new("user_99", "team_abc");
        let headers = HeaderMap::new();

        let err = authenticate_path_tenant(&fake, &headers, "team_abc/../other")
            .await
            .expect_err("a tenant carrying a path separator should be refused");

        assert!(matches!(err, ScopeError::InvalidTenant { .. }));
        assert_eq!(fake.identity_calls(), 0);
    }

    #[tokio::test]
    async fn an_invalid_foundation_name_is_rejected_before_authorization() {
        let fake = FakeAuth::new("user_99", "team_abc");
        let headers = HeaderMap::new();

        let err = authorize_scope(
            &fake,
            &headers,
            AuthzAction::ViewFoundation,
            &scope("team_abc", "my..db"),
        )
        .await
        .expect_err("an invalid name should be refused");

        assert!(matches!(err, ScopeError::InvalidFoundation { .. }));
        assert_eq!(err.code(), ErrorCodes::InvalidArgument);
        assert_eq!(fake.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn a_path_tenant_carrying_a_separator_is_rejected_before_authorization() {
        let fake = FakeAuth::new("user_99", "team_abc");
        let headers = HeaderMap::new();

        let err = authorize_scope(
            &fake,
            &headers,
            AuthzAction::ViewFoundation,
            &scope("team_abc/../other", "wiki_team"),
        )
        .await
        .expect_err("a tenant carrying a path separator should be refused");

        assert!(matches!(err, ScopeError::InvalidTenant { .. }));
        assert_eq!(err.code(), ErrorCodes::InvalidArgument);
        assert_eq!(fake.authorize_calls(), 0);
    }

    #[test]
    fn tenant_validator_accepts_a_uuid_and_rejects_separators() {
        assert_eq!(
            validate_path_tenant("2f1e0d9c-8b7a-4655-9443-2211aabbccdd"),
            Ok(())
        );
        assert_eq!(validate_path_tenant("default_tenant"), Ok(()));
        assert!(validate_path_tenant("").is_err());
        assert!(validate_path_tenant("a/b").is_err());
        assert!(validate_path_tenant("a?b").is_err());
        assert!(validate_path_tenant("a#b").is_err());
        // The ends rule is what refuses a bare `.` or `..`, so it has to hold
        // on a name that merely starts or finishes with a separator character.
        assert!(validate_path_tenant("_team").is_err());
        assert!(validate_path_tenant("team-").is_err());
    }

    #[test]
    fn tenant_validator_rejects_a_spelling_the_url_parser_would_rewrite() {
        // Each of these clears a check that forbids `/`, `?` and `#`, and each
        // is rewritten by the URL parser that builds the data-plane request.

        // A backslash is a separator to that parser, which then drops the `..`
        // segment beside it, so this one resolves to the tenant `victim`.
        assert!(validate_path_tenant("x\\..\\victim").is_err());
        // `%2e` decodes to `.`, so this resolves the way `..` does. A caller
        // writing `%252e%252e` reaches the validator as exactly this text.
        assert!(validate_path_tenant("%2e%2e").is_err());
        // A percent escape has no place in a tenant id whatever it spells, so
        // no second decoding round can reach a separator.
        assert!(validate_path_tenant("team%2F..%2Fother").is_err());
        // A segment the parser drops outright addresses a shorter path than
        // the URL spells.
        assert!(validate_path_tenant("..").is_err());
        assert!(validate_path_tenant(".").is_err());
        assert!(validate_path_tenant("team 1").is_err());
        // Two periods inside a segment are not a traversal, so the rule does
        // not reach further than it needs to.
        assert_eq!(validate_path_tenant("team..1"), Ok(()));
    }

    #[test]
    fn validator_accepts_a_plain_name() {
        assert_eq!(validate_foundation_name("wiki_team"), Ok(()));
    }

    #[test]
    fn validator_rejects_a_name_below_the_database_minimum() {
        assert!(validate_foundation_name("ab").is_err());
    }

    #[test]
    fn validator_rejects_a_topology_prefix() {
        // `validate_name` accepts a single `+` because a database name may
        // carry a topology prefix, so the Foundation rule has to reject it
        // separately or a name would address a different database than it
        // spells. Both halves still have to be valid names on their own, which
        // is why the suffix here is longer than the three-character minimum.
        assert!(chroma_types::validate_name("topo+database").is_ok());
        assert!(validate_foundation_name("topo+database").is_err());
    }

    #[test]
    fn validator_rejects_a_name_that_could_never_be_deleted() {
        let at_limit = "a".repeat(MAX_FOUNDATION_NAME_BYTES);
        let over_limit = "a".repeat(MAX_FOUNDATION_NAME_BYTES + 1);
        assert_eq!(validate_foundation_name(&at_limit), Ok(()));
        assert!(validate_foundation_name(&over_limit).is_err());
    }

    #[test]
    fn validator_rejects_consecutive_periods() {
        assert!(validate_foundation_name("my..db").is_err());
    }

    #[test]
    fn validator_rejects_the_reserved_crud_path_segment() {
        assert!(validate_foundation_name(RESERVED_FOUNDATION_NAME).is_err());
    }
}
