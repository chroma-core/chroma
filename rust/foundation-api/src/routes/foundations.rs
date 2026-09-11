//! Create, list and describe the Foundations one tenant holds.
//!
//! A Foundation is a Chroma database whose name is the Foundation's name, so
//! these routes are database operations dressed as Foundation ones: create
//! provisions a database and everything a Foundation is made of, list reports
//! the tenant's databases that hold a Foundation, and describe reports what one
//! of them holds.
//!
//! All three live under the static path segment `foundations`, which is why no
//! Foundation may be named that.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use chroma_api_types::GetUserIdentityResponse;
use chroma_sysdb::{DatabaseOrTopology, GetCollectionsOptions, SysDb};
use chroma_types::{
    AttachedFunction, AttachedFunctionUuid, Collection, CollectionUuid, DatabaseName,
    GetDatabaseError, SLACK_RAW_COLLECTION_NAME,
};
use serde::{Deserialize, Serialize};

use super::init::{
    foundation_attached_function_name, listed_attached_functions, provision_foundation,
    typed_attached_function, FoundationInitError, FoundationInitParams, FoundationInitResponse,
};
use super::whoami::{authorize_scope, authorize_tenant, ScopePolicy};
use super::FoundationScope;
use crate::{
    auth::{AuthenticateAndAuthorize, AuthzAction},
    errors::ServerError,
    server::FoundationApiServer,
};

/// The tenant named in the path of the create and list routes.
#[derive(Debug, Deserialize)]
pub struct TenantPath {
    pub tenant: String,
}

/// The tenant and Foundation named in the path of the describe route.
#[derive(Debug, Deserialize)]
pub struct FoundationPath {
    pub tenant: String,
    pub name: String,
}

/// Request body for `POST /api/f/{tenant}/foundations`.
#[derive(Debug, Deserialize)]
pub struct CreateFoundationRequest {
    /// Name of the Foundation, which becomes the name of its database.
    pub name: String,
}

/// One Foundation in a tenant's listing.
#[derive(Debug, Serialize)]
pub struct FoundationSummary {
    pub name: String,
    pub database_id: String,
}

/// Answer to `GET /api/f/{tenant}/foundations`, ordered by name.
#[derive(Debug, Serialize)]
pub struct ListFoundationsResponse {
    pub foundations: Vec<FoundationSummary>,
}

/// One collection a Foundation holds.
#[derive(Debug, Serialize)]
pub struct CollectionSummary {
    pub name: String,
    pub id: String,
}

/// What an attached function is doing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachedFunctionState {
    /// The function has failed at least once since it last succeeded.
    Failing,
    /// The function has produced its output collection, so at least one
    /// invocation has run to completion.
    Ready,
    /// The function is attached and has produced no output yet.
    Pending,
}

/// How far a function has consumed one of the collections it reads.
#[derive(Debug, Serialize)]
pub struct InputProgress {
    pub input_collection_id: String,
    /// Position in this input collection's log that the function has consumed
    /// up to. A position indexes one collection's log, so it is meaningful
    /// against an earlier reading of the same input and against nothing else.
    pub completion_offset: u64,
}

/// One function attached to a collection this Foundation holds.
#[derive(Debug, Serialize)]
pub struct AttachedFunctionSummary {
    pub id: String,
    pub name: String,
    pub output_collection_name: String,
    pub output_collection_id: Option<String>,
    pub state: AttachedFunctionState,
    /// Failures since the function last succeeded, taken as the highest count
    /// any one of its inputs reports rather than their sum.
    pub failure_count: i32,
    /// One entry per collection this Foundation holds that the function reads,
    /// ordered by collection id.
    pub inputs: Vec<InputProgress>,
}

/// Answer to `GET /api/f/{tenant}/foundations/{name}`.
#[derive(Debug, Serialize)]
pub struct DescribeFoundationResponse {
    pub tenant: String,
    pub name: String,
    pub database_id: String,
    /// Whether this database holds a Foundation at all.
    ///
    /// A tenant's key can address any database it owns, and a database becomes
    /// a Foundation only once it holds the wiki collection and the
    /// sources-to-wiki function. False therefore means "this database exists
    /// and is yours, and it is not a Foundation" — a distinct answer from the
    /// 404 a database that does not exist gets.
    ///
    /// The listing decides membership by this same question, so a name the
    /// listing reports describes as provisioned and a name it omits describes
    /// as not.
    pub provisioned: bool,
    pub collections: Vec<CollectionSummary>,
    pub functions: Vec<AttachedFunctionSummary>,
}

/// `POST /api/f/{tenant}/foundations` — provision a Foundation the caller
/// names.
///
/// Idempotent: every step of provisioning is get-or-create, so creating a
/// Foundation that already exists answers with the same ids and reports
/// `already_initialized: true` rather than a conflict.
#[tracing::instrument(name = "foundation_create", skip_all, err(Display))]
pub async fn foundation_create(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(path): Path<TenantPath>,
    Query(params): Query<FoundationInitParams>,
    Json(request): Json<CreateFoundationRequest>,
) -> Result<Json<FoundationInitResponse>, ServerError> {
    let scope = FoundationScope {
        tenant: Some(path.tenant),
        foundation: Some(request.name),
    };
    let default_database = &server.config.foundation.database_name;

    // Two permissions, checked separately, because neither alone is enough.
    // The create-database permission is the one that is scoped to a database, so
    // it is what confines a key to the names it may claim; a Foundation
    // permission names no database and so accepts any name. The Foundation
    // permission is what says this key may build a Foundation at all. Checking
    // only the first would let any key that can make a database make a
    // Foundation; checking only the second would let a key holding one
    // Foundation permission mint databases under any name in its tenant. A warm
    // authorization cache serves the second check without a second network
    // round trip.
    //
    // Neither check counts the databases the tenant already holds. That quota
    // is enforced by the frontend's create-database handler, which this route
    // does not go through.
    let (tenant, database, identity) = authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::CreateDatabase,
        &scope,
        default_database,
        ScopePolicy::Required,
    )
    .await?;
    authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::InitFoundation,
        &scope,
        default_database,
        ScopePolicy::Required,
    )
    .await?;

    // The Foundation belongs to the tenant in the path; the owner of the
    // private per-member collections is whoever called.
    let user_id = identity.user_id;
    let _guard =
        server.scorecard_request(&["op:foundation_create", &format!("tenant:{}", tenant)])?;

    let db_name = DatabaseName::new(&database).ok_or(FoundationInitError::DatabaseNameTooShort)?;
    tracing::info!(
        tenant = %tenant,
        user_id = %user_id,
        database = %database,
        mock_wiki = params.mock_wiki,
        "foundation create starting"
    );

    Ok(Json(
        provision_foundation(&server, tenant, user_id, db_name, params.mock_wiki).await?,
    ))
}

/// `GET /api/f/{tenant}/foundations` — the Foundations in a tenant that the
/// caller's key can address.
///
/// A database is a Foundation when it holds the wiki collection and carries the
/// sources-to-wiki attachment — the same predicate describe answers
/// `provisioned` with, so a name in this listing always describes as
/// provisioned. Both halves are load-bearing: a customer database that happens
/// to hold a collection named `wiki` is somebody's ordinary data, and
/// provisioning attaches the function only after creating the collections, so a
/// run that stopped partway leaves the collection standing without it.
///
/// Two system-database calls settle the candidates whatever the tenant's size:
/// one lists the tenant's databases, one finds every wiki collection in the
/// tenant. Each collection carries the database it lives in, so the second call
/// is not made per database.
///
/// The attachment cannot be settled that way. The gRPC contract filters
/// attachments by one input collection, by attachment id, or by attachment name
/// with nothing to scope the name to a tenant, so the one query that could cover
/// many databases at once would read every Foundation in the deployment and
/// discard all but this tenant's. Listing therefore reads each *candidate* on
/// its own — a database that holds a wiki collection and that the key reaches —
/// rather than every database the tenant holds.
///
/// Reading one candidate costs one call for its collections and then one call
/// per collection until the attachment turns up. Every collection has to be
/// reachable because the attachment is stored once per input collection and each
/// of those rows is retired on its own: deleting an input collection, or
/// detaching the function from it, retires that row and leaves the others
/// standing. The base input is read first, which is where provisioning puts the
/// attachment, so a Foundation usually costs two calls while a database that
/// merely holds a collection named `wiki` costs one per collection before it is
/// ruled out.
#[tracing::instrument(name = "foundation_list", skip_all, err(Display))]
pub async fn foundation_list(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(path): Path<TenantPath>,
) -> Result<Json<ListFoundationsResponse>, ServerError> {
    let identity = authorize_tenant(
        &*server.auth,
        &headers,
        AuthzAction::ViewFoundation,
        &path.tenant,
    )
    .await?;
    let tenant = path.tenant;
    let _guard =
        server.scorecard_request(&["op:foundation_list", &format!("tenant:{}", tenant)])?;

    let mut sysdb = server.sysdb.clone();
    // Every database, unpaged: the system database fetches the whole set and
    // slices it in memory, so paging here would cost the same and answer less.
    let databases = sysdb.list_databases(tenant.clone(), None, 0).await?;
    // One tenant-wide lookup for the wiki collection, with no database filter,
    // so a tenant with a hundred Foundations costs the same as one with two.
    // Each collection carries the database it lives in.
    let wiki_databases: HashSet<String> = sysdb
        .get_collections(GetCollectionsOptions {
            tenant: Some(tenant.clone()),
            name: Some(server.config.foundation.wiki_collection.clone()),
            ..Default::default()
        })
        .await?
        .into_iter()
        .map(|collection| collection.database)
        .collect();

    // The key's reach is applied before the per-candidate reads below, so a key
    // fenced to one Foundation reads one candidate however many the tenant
    // holds.
    let candidates = databases.into_iter().filter(|database| {
        wiki_databases.contains(&database.name)
            && key_reaches(&*server.auth, &identity, &database.name)
    });

    let mut foundations = Vec::new();
    for database in candidates {
        // A name too short to be a database name is a name create refuses, so
        // nothing under it was ever provisioned.
        let Some(db_name) = DatabaseName::new(&database.name) else {
            continue;
        };
        let collections = sysdb
            .get_collections(GetCollectionsOptions {
                tenant: Some(tenant.clone()),
                database_or_topology: Some(DatabaseOrTopology::Database(db_name)),
                ..Default::default()
            })
            .await?;
        if database_attaches_sources_to_wiki(&mut sysdb, &collections).await? {
            foundations.push(FoundationSummary {
                name: database.name,
                database_id: database.id.to_string(),
            });
        }
    }
    foundations.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(Json(ListFoundationsResponse { foundations }))
}

/// Whether any of one database's collections carries the sources-to-wiki
/// attachment.
///
/// Invariants:
/// 1. Every collection is reachable. The attachment is stored once per input
///    collection and each row is retired on its own, so a surviving row may sit
///    under any input and no single collection stands in for the database.
/// 2. The search stops at the first collection that carries it, so the answer
///    costs one call for a Foundation and one call per collection for a database
///    that is not one.
/// 3. The base input collection is read first, because provisioning creates the
///    attachment there. The order decides what the search costs and never what
///    it answers.
async fn database_attaches_sources_to_wiki(
    sysdb: &mut SysDb,
    collections: &[Collection],
) -> Result<bool, ServerError> {
    for collection_id in base_input_first(collections) {
        let attached = listed_attached_functions(sysdb, collection_id).await?;
        if attaches_sources_to_wiki(attached.iter().map(|function| function.name.as_str())) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// These collections' ids, the base input first and the rest in the order given.
fn base_input_first(collections: &[Collection]) -> Vec<CollectionUuid> {
    let (base, rest): (Vec<_>, Vec<_>) = collections
        .iter()
        .partition(|collection| collection.name == SLACK_RAW_COLLECTION_NAME);
    base.into_iter()
        .chain(rest)
        .map(|collection| collection.collection_id)
        .collect()
}

/// Whether these attachment names include the one that generates a wiki.
///
/// Invariant: this is the half of "is a Foundation" that the collections alone
/// cannot answer. The other half is the wiki collection, and both are required,
/// so a database holding only one of them is not a Foundation. List and describe
/// decide through this function, which is what keeps them from disagreeing about
/// a name.
fn attaches_sources_to_wiki<'a>(
    attached_function_names: impl IntoIterator<Item = &'a str>,
) -> bool {
    let sources_to_wiki = foundation_attached_function_name();
    attached_function_names
        .into_iter()
        .any(|name| name == sources_to_wiki)
}

/// `GET /api/f/{tenant}/foundations/{name}` — what one Foundation holds.
///
/// Answers 404 only when the tenant holds no database under this name. A
/// database that exists but was never provisioned answers 200 with
/// `provisioned: false`, so a caller can tell a name it has not used yet from a
/// name that is taken by something other than a Foundation.
///
/// Attachments are read per collection, so the system database is called once
/// for the database, once for its collections, and once more for each
/// collection. Two of a Foundation's collections are private to one member, so
/// that count grows with the number of members who have signed in.
#[tracing::instrument(name = "foundation_describe", skip_all, err(Display))]
pub async fn foundation_describe(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(path): Path<FoundationPath>,
) -> Result<Json<DescribeFoundationResponse>, ServerError> {
    let scope = FoundationScope {
        tenant: Some(path.tenant),
        foundation: Some(path.name),
    };
    let (tenant, database, identity) = authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::ViewFoundation,
        &scope,
        &server.config.foundation.database_name,
        ScopePolicy::Required,
    )
    .await?;
    let _guard =
        server.scorecard_request(&["op:foundation_describe", &format!("tenant:{}", tenant)])?;

    // A Foundation permission claim names no database, so the check above
    // settles the tenant and cannot separate one of its databases from another.
    // The key's reach is what does. Every other Foundation route proxies its
    // reads through the frontend, which re-checks the caller's data-plane claims
    // per collection; this one reads the system database with the service's own
    // credentials, so nothing downstream would catch a key reaching past its
    // own Foundation. A name outside the reach answers as absent rather than as
    // forbidden, so the answer does not confirm that the name exists.
    if !key_reaches(&*server.auth, &identity, &database) {
        return Err(FoundationInitError::FoundationNotFound { name: database }.into());
    }

    let db_name = DatabaseName::new(&database).ok_or(FoundationInitError::DatabaseNameTooShort)?;
    let mut sysdb = server.sysdb.clone();
    let stored = match sysdb.get_database(db_name.clone(), tenant.clone()).await {
        Ok(stored) => stored,
        Err(GetDatabaseError::NotFound(_)) => {
            return Err(FoundationInitError::FoundationNotFound { name: database }.into())
        }
        Err(error) => return Err(error.into()),
    };

    let collections = sysdb
        .get_collections(GetCollectionsOptions {
            tenant: Some(tenant.clone()),
            database_or_topology: Some(DatabaseOrTopology::Database(db_name)),
            ..Default::default()
        })
        .await?;

    // One function has a row per input collection, so the same function is
    // listed once under each of its inputs. Keying on the function id collapses
    // those rows into the one function they describe, folding the per-input
    // progress as it goes.
    let mut by_id: BTreeMap<_, FoldedFunction> = BTreeMap::new();
    for collection in &collections {
        for listed in listed_attached_functions(&mut sysdb, collection.collection_id).await? {
            let function = typed_attached_function(listed)?;
            match by_id.entry(function.id) {
                Entry::Vacant(slot) => {
                    slot.insert(FoldedFunction::from_row(function));
                }
                Entry::Occupied(mut held) => held.get_mut().fold_input_row(function),
            }
        }
    }

    let wiki_collection = &server.config.foundation.wiki_collection;
    let provisioned = collections
        .iter()
        .any(|collection| collection.name == *wiki_collection)
        && attaches_sources_to_wiki(by_id.values().map(|folded| folded.name.as_str()));

    Ok(Json(DescribeFoundationResponse {
        tenant,
        name: stored.name,
        database_id: stored.id.to_string(),
        provisioned,
        collections: collections
            .into_iter()
            .map(|collection| CollectionSummary {
                name: collection.name,
                id: collection.collection_id.to_string(),
            })
            .collect(),
        functions: by_id.into_values().map(summarize_function).collect(),
    }))
}

/// Whether the caller's key can address the Foundation `name`.
///
/// The reach is the union of the database names across every permission the key
/// holds, whichever permission named them. A tenant-wide key's permissions name
/// no database, so its reach is the empty set and every Foundation in the
/// tenant is addressable. A key scoped to databases reaches exactly those.
///
/// Reading the empty set as "all" rather than "none" is what keeps a
/// tenant-wide key working. Filtering at all is what confines a key fenced to
/// one Foundation, because a Foundation permission claim names no database and
/// so cannot confine anything by itself.
///
/// The empty set says only that no permission named a database, which is a
/// weaker statement than "this key is tenant-wide": a key scoped to one database
/// whose permissions all happen to be the kind that carry no database name
/// reaches every Foundation here too. The reach set is therefore a fence the
/// data plane already draws rather than one built for this route, and the route
/// takes it as it finds it.
///
/// A deployment whose authorization implementation grants no permissions has no
/// reach to read. Such an implementation answers every call with the same
/// placeholder identity, whose database name is a literal rather than a grant,
/// and filtering on it would fence every caller to that one name — hiding every
/// Foundation the deployment actually holds. The implementation says so through
/// [`AuthenticateAndAuthorize::enforces_permissions`], which is the only thing
/// that relaxes this filter; a deployment that does enforce permissions keeps it
/// whole.
fn key_reaches(
    auth: &dyn AuthenticateAndAuthorize,
    identity: &GetUserIdentityResponse,
    name: &str,
) -> bool {
    !auth.enforces_permissions()
        || identity.databases.is_empty()
        || identity.databases.contains(name)
}

/// One function, folded from the rows that pair it with each of its inputs.
///
/// The system database tracks a function's progress per input: the failure count
/// and the log position consumed are stored on the row pairing the function with
/// one of its inputs, and the listing answers with one such row per input.
///
/// The fold holds the function's own fields and drops the rest of each row,
/// because the two fields a row carries that describe one input rather than the
/// function — the input collection and the position consumed in it — are
/// meaningless once the rows are collapsed. Keeping a whole row would leave
/// whichever one arrived first standing in for all of them.
///
/// Invariants:
/// 1. `failure_count` is the highest any input reports, so a function failing on
///    every input but the one read first never reads as healthy.
/// 2. `output_collection_id` is the first one any row names, because a row
///    written before the function was marked ready names none.
/// 3. `id`, `name` and `output_collection_name` come from the first row read.
///    The system database updates an attachment by function id alone, without
///    naming an input, so every row for one function carries the same three.
/// 4. `offsets` holds one consumed position per input. A position indexes one
///    collection's log, so positions from two inputs measure different things
///    and are never reduced to a single number.
/// 5. `offsets` is ordered by input collection, so the reported inputs come back
///    in one order whatever order the system database answered the rows in.
struct FoldedFunction {
    id: AttachedFunctionUuid,
    name: String,
    output_collection_name: String,
    output_collection_id: Option<CollectionUuid>,
    failure_count: i32,
    offsets: BTreeMap<CollectionUuid, u64>,
}

impl FoldedFunction {
    /// The fold seeded with the first row read for this function.
    fn from_row(row: AttachedFunction) -> Self {
        Self {
            id: row.id,
            name: row.name,
            output_collection_name: row.output_collection_name,
            output_collection_id: row.output_collection_id,
            failure_count: row.failure_count,
            offsets: BTreeMap::from([(row.input_collection_id, row.completion_offset)]),
        }
    }

    /// Folds one more of this function's input rows in.
    fn fold_input_row(&mut self, row: AttachedFunction) {
        self.failure_count = self.failure_count.max(row.failure_count);
        self.output_collection_id = self.output_collection_id.or(row.output_collection_id);
        self.offsets
            .insert(row.input_collection_id, row.completion_offset);
    }
}

/// Derives what a function is doing from the two fields the stored row carries.
///
/// The stored row has no state column, so the state is read off two others.
/// Failure wins over readiness: the failure count is the failures since the
/// function last succeeded, so a positive count means the function is failing
/// now, while an output collection only records that some earlier invocation
/// finished.
fn function_state(
    output_collection_id: Option<CollectionUuid>,
    failure_count: i32,
) -> AttachedFunctionState {
    if failure_count > 0 {
        AttachedFunctionState::Failing
    } else if output_collection_id.is_some() {
        AttachedFunctionState::Ready
    } else {
        AttachedFunctionState::Pending
    }
}

fn summarize_function(folded: FoldedFunction) -> AttachedFunctionSummary {
    AttachedFunctionSummary {
        id: folded.id.to_string(),
        name: folded.name,
        output_collection_name: folded.output_collection_name,
        output_collection_id: folded
            .output_collection_id
            .map(|collection_id| collection_id.to_string()),
        state: function_state(folded.output_collection_id, folded.failure_count),
        failure_count: folded.failure_count,
        inputs: folded
            .offsets
            .into_iter()
            .map(|(input_collection_id, completion_offset)| InputProgress {
                input_collection_id: input_collection_id.to_string(),
                completion_offset,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::test_auth::{expect_ok, server_with, FakeAuth};
    use chroma_error::ErrorCodes;
    use chroma_sysdb::TestSysDb;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    const TENANT: &str = "team_1";

    fn collection(name: &str, database: &str) -> Collection {
        Collection {
            collection_id: CollectionUuid::new(),
            name: name.to_string(),
            tenant: TENANT.to_string(),
            database: database.to_string(),
            ..Default::default()
        }
    }

    fn tenant_path() -> Path<TenantPath> {
        Path(TenantPath {
            tenant: TENANT.to_string(),
        })
    }

    fn foundation_path(name: &str) -> Path<FoundationPath> {
        Path(FoundationPath {
            tenant: TENANT.to_string(),
            name: name.to_string(),
        })
    }

    fn identity_naming(databases: &[&str]) -> GetUserIdentityResponse {
        GetUserIdentityResponse {
            user_id: "user_1".to_string(),
            tenant: TENANT.to_string(),
            databases: databases.iter().map(|name| name.to_string()).collect(),
        }
    }

    /// A system database holding `collections`, and holding `database` as a
    /// database a create call put there.
    async fn sysdb_holding(database: &str, collections: Vec<Collection>) -> SysDb {
        let mut test = TestSysDb::new();
        for collection in collections {
            test.add_collection(collection);
        }
        let mut sysdb = SysDb::Test(test);
        sysdb
            .create_database(
                uuid::Uuid::new_v4(),
                DatabaseName::new(database).expect("test database name should be long enough"),
                TENANT.to_string(),
            )
            .await
            .expect("creating a database the tenant does not hold should succeed");
        sysdb
    }

    fn attached_function(name: &str, input_collection_id: CollectionUuid) -> AttachedFunction {
        AttachedFunction {
            id: AttachedFunctionUuid::new(),
            name: name.to_string(),
            function_id: uuid::Uuid::new_v4(),
            input_collection_id,
            output_collection_name: "wiki".to_string(),
            output_collection_id: None,
            params: None,
            tenant_id: TENANT.to_string(),
            database_id: "database".to_string(),
            last_run: None,
            completion_offset: 0,
            failure_count: 0,
            min_records_for_invocation: 1,
            is_deleted: false,
            is_async: true,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
        }
    }

    /// Puts what a provisioned Foundation holds into `test`: the wiki
    /// collection, the base input collection, and the sources-to-wiki
    /// attachment on the base input. Returns the attachment rather than storing
    /// it, because `set_attached_functions` replaces the whole set and so takes
    /// every database's attachments in one call.
    fn seed_foundation(test: &mut TestSysDb, database: &str) -> AttachedFunction {
        let base_input = collection(SLACK_RAW_COLLECTION_NAME, database);
        let function = attached_function(
            &foundation_attached_function_name(),
            base_input.collection_id,
        );
        test.add_collection(collection("wiki", database));
        test.add_collection(base_input);
        function
    }

    /// Stores every seeded attachment, keyed the way the system database keys
    /// them.
    fn store_attachments(test: &mut TestSysDb, functions: Vec<AttachedFunction>) {
        test.set_attached_functions(HashMap::from_iter(
            functions
                .into_iter()
                .map(|function| (function.input_collection_id, vec![function])),
        ));
    }

    #[tokio::test]
    async fn create_authorizes_the_new_name_under_both_permissions() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let server = server_with(fake.clone(), SysDb::Test(TestSysDb::new()));

        // No function endpoint is configured, so provisioning stops right after
        // both permission checks. What the route asked for is what is under
        // test here, not what it went on to build.
        let refused = foundation_create(
            HeaderMap::new(),
            State(server),
            tenant_path(),
            Query(FoundationInitParams::default()),
            Json(CreateFoundationRequest {
                name: "wiki_team".to_string(),
            }),
        )
        .await;
        assert!(
            refused.is_err(),
            "an unconfigured function endpoint should stop provisioning"
        );

        let authorizations = fake.authorizations();
        assert_eq!(
            authorizations.len(),
            2,
            "create must check both the create-database and the Foundation permission"
        );
        assert_eq!(authorizations[0].0, AuthzAction::CreateDatabase);
        assert_eq!(authorizations[1].0, AuthzAction::InitFoundation);
        for (action, resource) in &authorizations {
            assert_eq!(resource.tenant.as_deref(), Some(TENANT), "{action}");
            // Both checks name the database being created, not the configured
            // default one, so a key scoped away from this name is refused.
            assert_eq!(resource.database.as_deref(), Some("wiki_team"), "{action}");
            assert_eq!(resource.collection, None, "{action}");
        }
        // The tenant is in the path, so it is never looked up.
        assert_eq!(fake.identity_calls(), 0);
    }

    #[tokio::test]
    async fn create_refuses_an_illegal_name_before_authorizing() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let server = server_with(fake.clone(), SysDb::Test(TestSysDb::new()));

        let refused = foundation_create(
            HeaderMap::new(),
            State(server),
            tenant_path(),
            Query(FoundationInitParams::default()),
            Json(CreateFoundationRequest {
                name: "foundations".to_string(),
            }),
        )
        .await;

        match refused {
            Ok(_) => panic!("the reserved name must not be creatable"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::InvalidArgument),
        }
        assert_eq!(fake.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn list_authorizes_the_tenant_and_names_no_database() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let server = server_with(fake.clone(), SysDb::Test(TestSysDb::new()));

        let _listed = expect_ok(
            foundation_list(HeaderMap::new(), State(server), tenant_path()).await,
            "listing an empty tenant should succeed",
        );

        assert_eq!(fake.captured_action(), AuthzAction::ViewFoundation);
        let resource = fake.captured_resource();
        assert_eq!(resource.tenant.as_deref(), Some(TENANT));
        // A listing addresses the set of Foundations, not one of them.
        assert_eq!(resource.database, None);
        assert_eq!(resource.collection, None);
    }

    #[tokio::test]
    async fn list_reports_only_the_databases_that_hold_a_provisioned_foundation() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mut test = TestSysDb::new();
        let alpha = seed_foundation(&mut test, "alpha");
        test.add_collection(collection("notion", "beta"));
        store_attachments(&mut test, vec![alpha]);
        let server = server_with(fake, SysDb::Test(test));

        let listed = expect_ok(
            foundation_list(HeaderMap::new(), State(server), tenant_path()).await,
            "listing should succeed",
        );

        let names: Vec<&str> = listed
            .foundations
            .iter()
            .map(|foundation| foundation.name.as_str())
            .collect();
        assert_eq!(names, vec!["alpha"]);
    }

    #[tokio::test]
    async fn list_hides_a_database_that_only_holds_a_collection_named_wiki() {
        // `wiki` is an ordinary collection name a customer may use for its own
        // data. Listing such a database would put a name in the listing that
        // describe then reports as not provisioned, so the two routes decide
        // membership the same way.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let mut test = TestSysDb::new();
        let alpha = seed_foundation(&mut test, "alpha");
        test.add_collection(collection("wiki", "customer_db"));
        test.add_collection(collection(SLACK_RAW_COLLECTION_NAME, "customer_db"));
        store_attachments(&mut test, vec![alpha]);
        let server = server_with(fake, SysDb::Test(test));

        let listed = expect_ok(
            foundation_list(HeaderMap::new(), State(server), tenant_path()).await,
            "listing should succeed",
        );

        let names: Vec<&str> = listed
            .foundations
            .iter()
            .map(|foundation| foundation.name.as_str())
            .collect();
        assert_eq!(names, vec!["alpha"]);
    }

    #[tokio::test]
    async fn list_finds_an_attachment_that_outlived_its_base_input() {
        // The attachment is stored once per input collection, and the system
        // database retires those rows one at a time: deleting an input
        // collection, or detaching the function from it, leaves the rows under
        // the other inputs standing. A search that read only the base input
        // would call this Foundation absent while describe called it
        // provisioned.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let notion = collection("notion", "wiki_team");
        let surviving_row =
            attached_function(&foundation_attached_function_name(), notion.collection_id);
        let sysdb = sysdb_holding("wiki_team", vec![collection("wiki", "wiki_team"), notion]).await;
        let SysDb::Test(mut test) = sysdb.clone() else {
            panic!("the test system database should be the test one");
        };
        store_attachments(&mut test, vec![surviving_row]);
        let server = server_with(fake, sysdb);

        let listed = expect_ok(
            foundation_list(HeaderMap::new(), State(server.clone()), tenant_path()).await,
            "listing should succeed",
        );
        assert_eq!(
            listed
                .foundations
                .iter()
                .map(|foundation| foundation.name.as_str())
                .collect::<Vec<_>>(),
            vec!["wiki_team"]
        );

        let described = expect_ok(
            foundation_describe(
                HeaderMap::new(),
                State(server),
                foundation_path("wiki_team"),
            )
            .await,
            "describing should succeed",
        );
        assert!(described.provisioned);
    }

    #[tokio::test]
    async fn list_and_describe_agree_about_a_database_that_holds_no_attachment() {
        // The two routes answer one question, so a name absent from the listing
        // must be a name describe calls unprovisioned, and the other way round.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let sysdb = sysdb_holding(
            "customer_db",
            vec![
                collection("wiki", "customer_db"),
                collection(SLACK_RAW_COLLECTION_NAME, "customer_db"),
            ],
        )
        .await;
        let server = server_with(fake, sysdb);

        let listed = expect_ok(
            foundation_list(HeaderMap::new(), State(server.clone()), tenant_path()).await,
            "listing should succeed",
        );
        assert!(listed.foundations.is_empty());

        let described = expect_ok(
            foundation_describe(
                HeaderMap::new(),
                State(server),
                foundation_path("customer_db"),
            )
            .await,
            "describing a database the tenant holds should succeed",
        );
        assert!(!described.provisioned);
    }

    #[tokio::test]
    async fn list_narrows_to_the_databases_the_key_names() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT).scoped_to_databases(&["beta"]));
        let mut test = TestSysDb::new();
        let alpha = seed_foundation(&mut test, "alpha");
        let beta = seed_foundation(&mut test, "beta");
        store_attachments(&mut test, vec![alpha, beta]);
        let server = server_with(fake, SysDb::Test(test));

        let listed = expect_ok(
            foundation_list(HeaderMap::new(), State(server), tenant_path()).await,
            "listing should succeed",
        );

        let names: Vec<&str> = listed
            .foundations
            .iter()
            .map(|foundation| foundation.name.as_str())
            .collect();
        assert_eq!(names, vec!["beta"]);
    }

    #[tokio::test]
    async fn describe_authorizes_the_named_foundation() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let sysdb = sysdb_holding("wiki_team", vec![]).await;
        let server = server_with(fake.clone(), sysdb);

        let _described = expect_ok(
            foundation_describe(
                HeaderMap::new(),
                State(server),
                foundation_path("wiki_team"),
            )
            .await,
            "describing a database the tenant holds should succeed",
        );

        assert_eq!(fake.captured_action(), AuthzAction::ViewFoundation);
        let resource = fake.captured_resource();
        assert_eq!(resource.tenant.as_deref(), Some(TENANT));
        assert_eq!(resource.database.as_deref(), Some("wiki_team"));
    }

    #[tokio::test]
    async fn describe_answers_not_found_for_a_name_the_tenant_does_not_hold() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let server = server_with(fake, SysDb::Test(TestSysDb::new()));

        let refused = foundation_describe(
            HeaderMap::new(),
            State(server),
            foundation_path("wiki_team"),
        )
        .await;

        match refused {
            Ok(_) => panic!("a database the tenant does not hold must not describe"),
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::NotFound),
        }
    }

    #[tokio::test]
    async fn describe_separates_a_plain_database_from_a_provisioned_foundation() {
        // A tenant's key can address any database it owns, so describe has to
        // answer for a database that is not a Foundation rather than pretend it
        // is absent.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let sysdb = sysdb_holding("plain_db", vec![collection("notion", "plain_db")]).await;
        let server = server_with(fake, sysdb);

        let described = expect_ok(
            foundation_describe(HeaderMap::new(), State(server), foundation_path("plain_db")).await,
            "describing a database the tenant holds should succeed",
        );

        assert!(!described.provisioned);
        assert_eq!(described.name, "plain_db");
        assert_eq!(
            described
                .collections
                .iter()
                .map(|collection| collection.name.as_str())
                .collect::<Vec<_>>(),
            vec!["notion"]
        );
        assert!(described.functions.is_empty());
    }

    #[tokio::test]
    async fn describe_reports_a_provisioned_foundation_and_lists_each_function_once() {
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let wiki = collection("wiki", "wiki_team");
        let slack_raw = collection("slack_raw", "wiki_team");
        let notion = collection("notion", "wiki_team");

        // The sources-to-wiki function has one row per input collection, and
        // both rows describe the same function.
        let sources_to_wiki = attached_function(
            &foundation_attached_function_name(),
            slack_raw.collection_id,
        );
        let second_input = AttachedFunction {
            input_collection_id: notion.collection_id,
            ..sources_to_wiki.clone()
        };

        let mut test = TestSysDb::new();
        test.set_attached_functions(HashMap::from([
            (slack_raw.collection_id, vec![sources_to_wiki]),
            (notion.collection_id, vec![second_input]),
        ]));
        for held in [wiki, slack_raw, notion] {
            test.add_collection(held);
        }
        let mut sysdb = SysDb::Test(test);
        sysdb
            .create_database(
                uuid::Uuid::new_v4(),
                DatabaseName::new("wiki_team").expect("name should be long enough"),
                TENANT.to_string(),
            )
            .await
            .expect("creating the database should succeed");
        let server = server_with(fake, sysdb);

        let described = expect_ok(
            foundation_describe(
                HeaderMap::new(),
                State(server),
                foundation_path("wiki_team"),
            )
            .await,
            "describing a provisioned Foundation should succeed",
        );

        assert!(described.provisioned);
        assert_eq!(described.collections.len(), 3);
        assert_eq!(
            described.functions.len(),
            1,
            "one function attached to two inputs is one function"
        );
        assert_eq!(
            described.functions[0].name,
            foundation_attached_function_name()
        );
        assert_eq!(described.functions[0].state, AttachedFunctionState::Pending);
    }

    #[tokio::test]
    async fn describe_hides_a_database_the_key_cannot_reach() {
        // A Foundation permission claim names no database, so authorization
        // alone lets a key fenced to one Foundation name any database in its
        // tenant. Describe reads the system database directly, so nothing
        // downstream would catch that; the key's reach has to.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT).scoped_to_databases(&["wiki_team"]));
        let sysdb = sysdb_holding("customer_db", vec![collection("orders", "customer_db")]).await;
        let server = server_with(fake.clone(), sysdb);

        let refused = foundation_describe(
            HeaderMap::new(),
            State(server),
            foundation_path("customer_db"),
        )
        .await;

        match refused {
            Ok(_) => panic!("a database outside the key's reach must not describe"),
            // Absent, not forbidden: the answer must not confirm the name
            // exists.
            Err(error) => assert_eq!(error.0.code(), ErrorCodes::NotFound),
        }
        // The refusal happens after the permission check, which is what makes
        // it a narrowing of an authorized request rather than a second gate.
        assert_eq!(fake.authorize_calls(), 1);
    }

    #[tokio::test]
    async fn describe_reports_the_least_healthy_of_a_function_s_inputs() {
        // The failure count is stored per input, so a function failing on one
        // input and healthy on another must not read as healthy. The consumed
        // position is stored per input too, but each one indexes a different
        // collection's log, so it is reported per input rather than reduced.
        let fake = Arc::new(FakeAuth::new("user_1", TENANT));
        let wiki = collection("wiki", "wiki_team");
        let slack_raw = collection("slack_raw", "wiki_team");
        let notion = collection("notion", "wiki_team");

        let healthy = AttachedFunction {
            completion_offset: 90,
            failure_count: 0,
            output_collection_id: Some(wiki.collection_id),
            ..attached_function(
                &foundation_attached_function_name(),
                slack_raw.collection_id,
            )
        };
        let failing = AttachedFunction {
            input_collection_id: notion.collection_id,
            completion_offset: 12,
            failure_count: 4,
            ..healthy.clone()
        };

        let mut test = TestSysDb::new();
        test.set_attached_functions(HashMap::from([
            (slack_raw.collection_id, vec![healthy]),
            (notion.collection_id, vec![failing]),
        ]));
        let slack_raw_id = slack_raw.collection_id;
        let notion_id = notion.collection_id;
        for held in [wiki, slack_raw, notion] {
            test.add_collection(held);
        }
        let mut sysdb = SysDb::Test(test);
        sysdb
            .create_database(
                uuid::Uuid::new_v4(),
                DatabaseName::new("wiki_team").expect("name should be long enough"),
                TENANT.to_string(),
            )
            .await
            .expect("creating the database should succeed");
        let server = server_with(fake, sysdb);

        let described = expect_ok(
            foundation_describe(
                HeaderMap::new(),
                State(server),
                foundation_path("wiki_team"),
            )
            .await,
            "describing a provisioned Foundation should succeed",
        );

        assert_eq!(described.functions.len(), 1);
        assert_eq!(described.functions[0].state, AttachedFunctionState::Failing);
        assert_eq!(described.functions[0].failure_count, 4);

        // Each input keeps its own position, labelled with the log it indexes,
        // and the entries come back ordered by collection id whatever order the
        // system database answered in.
        let mut expected = vec![(slack_raw_id, 90u64), (notion_id, 12u64)];
        expected.sort_by_key(|(collection_id, _)| *collection_id);
        let reported: Vec<(CollectionUuid, u64)> = described.functions[0]
            .inputs
            .iter()
            .map(|input| {
                let collection_id = input
                    .input_collection_id
                    .parse::<CollectionUuid>()
                    .expect("an input collection id should parse");
                (collection_id, input.completion_offset)
            })
            .collect();
        assert_eq!(reported, expected);
    }

    #[test]
    fn a_tenant_wide_key_reaches_every_foundation_and_a_scoped_one_reaches_its_own() {
        let enforcing = FakeAuth::new("user_1", TENANT);

        let tenant_wide = identity_naming(&[]);
        assert!(key_reaches(&enforcing, &tenant_wide, "alpha"));
        assert!(key_reaches(&enforcing, &tenant_wide, "beta"));

        let scoped = identity_naming(&["beta"]);
        assert!(!key_reaches(&enforcing, &scoped, "alpha"));
        assert!(key_reaches(&enforcing, &scoped, "beta"));
    }

    #[test]
    fn a_deployment_that_enforces_no_permissions_reaches_every_foundation() {
        // The no-op implementation hands back a placeholder identity naming one
        // literal database. Read as a reach it would fence every caller to that
        // one name, so list would answer with nothing and describe would call
        // every Foundation absent.
        let placeholder = identity_naming(&["default_database"]);
        assert!(key_reaches(&(), &placeholder, "alpha"));
        assert!(key_reaches(&(), &placeholder, "default_database"));
    }

    #[test]
    fn function_state_reads_failure_before_readiness() {
        let output = Some(CollectionUuid::new());
        assert_eq!(function_state(None, 0), AttachedFunctionState::Pending);
        assert_eq!(function_state(output, 0), AttachedFunctionState::Ready);
        assert_eq!(function_state(None, 3), AttachedFunctionState::Failing);
        // The count is failures since the last success, so a function that
        // produced output earlier and is failing now reads as failing.
        assert_eq!(function_state(output, 3), AttachedFunctionState::Failing);
    }
}
