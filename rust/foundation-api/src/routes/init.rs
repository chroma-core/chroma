use super::init_schema::{
    foundation_collection_schema, qwen_embedding_function, splade_embedding_function,
    CollectionEmbeddingFunctions,
};
use super::whoami::{authorize_scope, ScopePolicy};
use super::FoundationScope;
use crate::collections::{create_planned_collection, ensure_database, ensure_slack_raw_collection};
use crate::{
    auth::AuthzAction, config::FoundationConfig, errors::ServerError, server::FoundationApiServer,
};
use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_types::{
    AttachedFunction, Collection, CollectionUuid, DatabaseName, ListAttachedFunctionsError,
    Metadata, MetadataValue, Schema, CHROMA_GROUP_CHUNK_SIBLINGS_KEY, SLACK_RAW_COLLECTION_NAME,
};
use frontend_core::{attached_function_ops, foundation::source_kind_for_collection_name};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize)]
pub struct FoundationInitResponse {
    pub tenant: String,
    pub user_id: String,
    pub database: String,
    pub database_id: String,
    pub wiki_collection_id: String,
    pub trajectories_collection_id: String,
    pub wiki_revisions_collection_id: String,
    pub currents_collection_id: String,
    pub file_uploads_collection_id: String,
    pub agent_sessions_collection_id: String,
    /// Id of the `slack_raw` append-log collection. Metadata is
    /// inverted-indexed for filtering; text/vector indexing is deferred
    /// downstream. Wired as the attached function's base input in place of
    /// the old indexed `slack` source.
    pub slack_raw_collection_id: String,
    /// Whether this workspace had already been set up before the call.
    ///
    /// True when the shared foundation function was already attached, which
    /// only happens on a workspace a previous `/init` completed. Everything
    /// `/init` does is get-or-create, so a caller cannot otherwise tell a
    /// first-time setup from a repeat, and the two deserve different words:
    /// "workspace ready" reads wrong to someone who just created one, and
    /// "created your workspace" reads wrong to everyone else.
    pub already_initialized: bool,
    /// Name -> id for each ensured INDEXED source collection
    /// (notion, gdrive, …). Each carries the chunk-sibling grouping flag.
    pub source_collection_ids: std::collections::HashMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct FoundationInitParams {
    /// Use the separately configured mock-wiki Modal endpoint for the
    /// endpoint-backed attached functions created during initialization.
    #[serde(default)]
    pub mock_wiki: bool,
}

/// `POST /api/init` — idempotent bootstrap for a team's Foundation
/// workspace. Ensures the configured Foundation database and the wiki +
/// wiki_revisions collections (names overridable via
/// `CHROMA_FOUNDATION__*` env vars) exist in the tenant resolved from the
/// auth context. Pass `?mock_wiki=true` to select the separately configured
/// mock-wiki endpoint for endpoint-backed attached functions. Safe to call
/// repeatedly.
#[tracing::instrument(name = "foundation_init", skip_all, err(Display))]
pub async fn foundation_init(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(scope): Path<FoundationScope>,
    Query(params): Query<FoundationInitParams>,
) -> Result<Json<FoundationInitResponse>, ServerError> {
    let (tenant, database, identity) = authorize_scope(
        &*server.auth,
        &headers,
        AuthzAction::InitFoundation,
        &scope,
        &server.config.foundation.database_name,
        ScopePolicy::DefaultToConfig,
    )
    .await?;
    // The workspace belongs to the tenant in the path, but the owner recorded
    // in the response is whoever called, which only the identity knows.
    let user_id = identity.user_id;
    tracing::info!(
        tenant = %tenant,
        user_id = %user_id,
        database = %database,
        mock_wiki = params.mock_wiki,
        "foundation init starting"
    );

    let _guard =
        server.scorecard_request(&["op:foundation_init", &format!("tenant:{}", tenant)])?;

    let db_name = DatabaseName::new(&database).ok_or(FoundationInitError::DatabaseNameTooShort)?;
    Ok(Json(
        provision_foundation(&server, tenant, user_id, db_name, params.mock_wiki).await?,
    ))
}

/// Build every database, collection and attached function one Foundation is
/// made of, and report what it now holds.
///
/// Invariants:
/// 1. Every step is get-or-create, so calling this twice on the same
///    (tenant, database) pair leaves one Foundation and answers with the same
///    ids. This is what lets both the initialize and the create route run it
///    unguarded.
/// 2. `already_initialized` in the answer is the only field that separates a
///    first call from a repeat, and it is read from the attachment on the
///    `slack_raw` collection, which is per-database. A Foundation that was
///    never provisioned reports `false` even when its tenant holds other
///    Foundations.
/// 3. `user_id` names the caller, not the tenant. Two collections are private
///    to one member and carry the id in their names, so passing another user's
///    id provisions that user's private collections instead.
///
/// Authorization is the caller's to do: this touches sysdb directly and checks
/// no permission of its own.
pub(crate) async fn provision_foundation(
    server: &FoundationApiServer,
    tenant: String,
    user_id: String,
    db_name: DatabaseName,
    mock_wiki: bool,
) -> Result<FoundationInitResponse, ServerError> {
    let database = db_name.as_ref().to_string();
    let foundation_cfg = &server.config.foundation;
    let function_endpoint_url = configured_function_endpoint_url(foundation_cfg, mock_wiki)?;

    let mut sysdb = server.sysdb.clone();
    let database_id = ensure_database(&mut sysdb, db_name.clone(), tenant.clone()).await?;

    // Wiki collections are the attached function's *output*; they don't
    // need chunk-sibling grouping (no end-of-job marker is read from them).
    let wiki = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &foundation_cfg.wiki_collection,
        None,
        // NOTE(hammadb): Foundation uses Qwen0.6B by default which is 1024 dims
        Some(1024),
        CollectionEmbeddingFunctions {
            dense: Some(qwen_embedding_function()),
            sparse: Some(splade_embedding_function()),
        },
    )
    .await?;
    let wiki_revisions = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &foundation_cfg.wiki_revisions_collection,
        None,
        Some(1),
        CollectionEmbeddingFunctions::default(),
    )
    .await?;
    // Generated trajectory records are structured KV documents keyed and
    // queried by metadata, not semantically searched, so they use the same
    // one-dimensional metadata-only shape as currents/wiki_revisions.
    let trajectories = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &foundation_cfg.trajectories_collection,
        None,
        Some(1),
        CollectionEmbeddingFunctions::default(),
    )
    .await?;
    // Currents records carry their payload in metadata and are only ever
    // fetched by metadata (never vector-searched), so the collection has no
    // embedding function. Pin the dense index to a single dimension for the
    // derived records written by the currents function.
    let currents =
        ensure_currents_collection(&mut sysdb, tenant.clone(), db_name.clone(), foundation_cfg)
            .await?;

    // Attach revision_history to the wiki collection so every mutation is
    // archived into wiki_revisions automatically on compaction.
    ensure_revision_history_function(
        &mut sysdb,
        tenant.clone(),
        &db_name,
        wiki.collection_id,
        foundation_cfg,
    )
    .await?;
    if foundation_cfg.enable_currents_function {
        ensure_currents_function(
            &mut sysdb,
            tenant.clone(),
            &db_name,
            wiki.collection_id,
            foundation_cfg,
            function_endpoint_url,
        )
        .await?;
    }
    // Private (per-user) collections — namespaced by user_id so each team
    // member gets their own isolated collection for uploads and traces.
    let file_uploads_name = format!("{}_{}", foundation_cfg.file_uploads_collection, user_id);
    let file_uploads = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &file_uploads_name,
        None,
        Some(1024),
        CollectionEmbeddingFunctions {
            dense: Some(qwen_embedding_function()),
            sparse: Some(splade_embedding_function()),
        },
    )
    .await?;

    // The agent_sessions collection is wired into the sources->wiki function
    // below, so it carries the chunk-sibling grouping flag like the other
    // source collections (keeps a job's chunk records in one partition and
    // surfaces the trailing end-of-job marker after every sibling chunk).
    let agent_sessions_name = format!("{}_{}", foundation_cfg.agent_sessions_collection, user_id);
    let agent_sessions = ensure_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        &agent_sessions_name,
        Some(group_chunk_siblings_metadata()),
        Some(1024),
        CollectionEmbeddingFunctions {
            dense: Some(qwen_embedding_function()),
            sparse: Some(splade_embedding_function()),
        },
    )
    .await?;

    // Real-time Slack messages land in `slack_raw` as raw, single records (an
    // append log). Metadata (channel/team/thread/op) is inverted-indexed so
    // records are filterable at read time, but text/vector indexing —
    // batching, rendering, embedding — is deferred to the attached function
    // downstream, so the collection has no FTS/vector indexes, no embedding
    // function, and no dimension. It also does NOT carry the chunk-sibling
    // grouping flag: each message is its own single record, so there are no
    // sibling chunks to keep in one partition.
    //
    // Created BEFORE the config-driven indexed-source loop below: collection
    // creation is GET_OR_CREATE (first writer wins), so ensuring `slack_raw`
    // first guarantees its hybrid schema even if a misconfigured
    // `indexed_source_collections` also lists `slack_raw` — the loop would
    // then get this collection back unchanged instead of creating it with the
    // fully indexed schema. Same protection the other fixed collections
    // (wiki, currents, agent_sessions) already get from preceding the loop.
    let slack_raw = ensure_slack_raw_collection(
        &mut sysdb,
        tenant.clone(),
        db_name.clone(),
        SLACK_RAW_COLLECTION_NAME,
        None,
    )
    .await?;

    // Indexed source collections (notion, gdrive, …) are *extra* inputs to the
    // attached function. They carry the chunk-sibling grouping flag so a job's
    // chunk records stay in one partition and the trailing end-of-job marker on
    // `{base}-0` is observed after every sibling chunk (ADR 0001 §6). All
    // inputs share one async attached function; extras are added via
    // `add_input()` below.
    let mut source_collection_ids = HashMap::new();
    let mut indexed_source_collections = Vec::new();
    for source_name in &foundation_cfg.indexed_source_collections {
        let source = ensure_collection(
            &mut sysdb,
            tenant.clone(),
            db_name.clone(),
            source_name,
            Some(group_chunk_siblings_metadata()),
            source_dimension(source_name),
            CollectionEmbeddingFunctions::default(),
        )
        .await?;
        indexed_source_collections.push((source_name.clone(), source.collection_id));
        source_collection_ids.insert(source_name.clone(), source.collection_id.to_string());
    }

    // `slack_raw` is the attached function's *base* input, in place of the old
    // indexed `slack` source. It is the right base for two reasons:
    //  - it is always created (above), so the function is always created and
    //    every input is wired even when `indexed_source_collections` is
    //    empty; and
    //  - it is a fixed collection, so the base — which keys the function's
    //    identity in sysdb — is stable across `indexed_source_collections`
    //    changes, keeping repeated `/init` calls idempotent.
    // Its source_kind resolves to `slack`, so the generation contract is
    // unchanged from the old `slack` base.
    let already_initialized = ensure_attached_function(
        &mut sysdb,
        tenant.clone(),
        &db_name,
        slack_raw.collection_id,
        SLACK_RAW_COLLECTION_NAME,
        foundation_cfg,
        function_endpoint_url,
    )
    .await?;

    // Add the indexed sources and the per-user coding-agent traces collection
    // as extra inputs to the same sources->wiki function, so they flow into the
    // shared wiki output alongside slack_raw.
    for (_, source_collection_id) in &indexed_source_collections {
        attached_function_ops::add_attached_function_input(
            &mut sysdb,
            foundation_attached_function_name(),
            slack_raw.collection_id,
            *source_collection_id,
            db_name.clone(),
        )
        .await?;
    }
    attached_function_ops::add_attached_function_input(
        &mut sysdb,
        foundation_attached_function_name(),
        slack_raw.collection_id,
        agent_sessions.collection_id,
        db_name.clone(),
    )
    .await?;

    tracing::info!(
        tenant = %tenant,
        database = %database,
        num_indexed_source_collections = source_collection_ids.len(),
        "foundation provisioning complete"
    );

    Ok(FoundationInitResponse {
        tenant,
        user_id,
        database,
        database_id: database_id.to_string(),
        wiki_collection_id: wiki.collection_id.to_string(),
        trajectories_collection_id: trajectories.collection_id.to_string(),
        wiki_revisions_collection_id: wiki_revisions.collection_id.to_string(),
        currents_collection_id: currents.collection_id.to_string(),
        file_uploads_collection_id: file_uploads.collection_id.to_string(),
        agent_sessions_collection_id: agent_sessions.collection_id.to_string(),
        slack_raw_collection_id: slack_raw.collection_id.to_string(),
        already_initialized,
        source_collection_ids,
    })
}

/// Dense-index dimensionality to pin a source collection to.
///
/// Sources split into two groups. Most of them — notion, and the per-user
/// agent-session collections — carry real 1024-dim vectors that the writer
/// computes and upserts. The backend-driven sources, Google Drive and
/// Granola, carry no vectors of their own: their connectors run with
/// embedding inference disabled and upsert a 1-element placeholder, because
/// a Chroma record needs at least one dimension and these collections are
/// read by the wiki's attached function rather than by vector search.
///
/// Pinning that second group to a single dimension is what lets their
/// writes land at all. A collection pinned to 1024 rejects every
/// placeholder upsert with a dimension mismatch, and because a
/// collection's dimension is fixed at creation, the rejection is
/// permanent. Add a source here whenever its connector is configured for
/// no embedding (`DenseEmbeddingModel::NoEmbed`, in hosted-chroma).
fn source_dimension(source_name: &str) -> Option<i32> {
    match source_kind_for_collection_name(source_name) {
        Ok("google_drive") | Ok("granola") => Some(1),
        _ => Some(1024),
    }
}

/// Collection metadata that opts a source collection into chunk-sibling
/// grouping during compaction/partitioning (see
/// [`chroma_types::CHROMA_GROUP_CHUNK_SIBLINGS_KEY`]).
fn group_chunk_siblings_metadata() -> Metadata {
    let mut metadata = HashMap::new();
    metadata.insert(
        CHROMA_GROUP_CHUNK_SIBLINGS_KEY.to_string(),
        MetadataValue::Bool(true),
    );
    metadata
}

/// Raised when `/init` needs the selected attached-function endpoint URL but
/// the deployment never configured it. Surfaced as a 500 so a misconfigured
/// deploy fails loudly instead of attaching a function with a
/// missing/placeholder endpoint.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum MissingFunctionEndpointUrl {
    #[error("foundation.function_endpoint_url is not configured")]
    Default,
    #[error("foundation.mock_wiki_function_endpoint_url is not configured")]
    MockWiki,
}

impl ChromaError for MissingFunctionEndpointUrl {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

fn configured_function_endpoint_url(
    cfg: &FoundationConfig,
    mock_wiki: bool,
) -> Result<&str, MissingFunctionEndpointUrl> {
    if mock_wiki {
        cfg.mock_wiki_function_endpoint_url
            .as_deref()
            .ok_or(MissingFunctionEndpointUrl::MockWiki)
    } else {
        cfg.function_endpoint_url
            .as_deref()
            .ok_or(MissingFunctionEndpointUrl::Default)
    }
}

/// Reject an init mode that disagrees with the endpoint already persisted on
/// an attached function. The worker treats a trailing slash as insignificant
/// when it constructs route URLs, so init does the same for this comparison.
fn validate_persisted_function_endpoint(
    function: &AttachedFunction,
    selected_endpoint_url: &str,
) -> Result<(), PersistedFunctionEndpointError> {
    let persisted_endpoint_url = function
        .params
        .as_deref()
        .and_then(|params| serde_json::from_str::<serde_json::Value>(params).ok())
        .and_then(|params| {
            params
                .get("endpoint_url")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .ok_or_else(|| PersistedFunctionEndpointError::MissingOrInvalid {
            attached_function: function.name.clone(),
        })?;

    if persisted_endpoint_url.trim_end_matches('/') != selected_endpoint_url.trim_end_matches('/') {
        return Err(PersistedFunctionEndpointError::Conflict {
            attached_function: function.name.clone(),
            persisted_endpoint_url,
            selected_endpoint_url: selected_endpoint_url.to_string(),
        });
    }

    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum PersistedFunctionEndpointError {
    #[error(
        "attached function {attached_function} has no valid persisted endpoint_url; cannot verify the requested init mode"
    )]
    MissingOrInvalid { attached_function: String },
    #[error(
        "attached function {attached_function} is persisted with endpoint {persisted_endpoint_url}, but this init request selected {selected_endpoint_url}"
    )]
    Conflict {
        attached_function: String,
        persisted_endpoint_url: String,
        selected_endpoint_url: String,
    },
}

impl ChromaError for PersistedFunctionEndpointError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::FailedPrecondition
    }
}

/// Idempotently create the shared foundation function on the base source
/// collection. Additional source collections are attached later via
/// `add_input()`. Params carry the modal endpoint and the base source kind,
/// matching the existing Foundation function contract.
///
/// `/init` is safe to call repeatedly: an attachment that is already there is
/// left alone. Checking here is what makes that true — see
/// [`attached_function_by_name`] for why the layer below cannot.
///
/// Returns whether the attachment was already present, which is what tells the
/// caller this workspace had been set up before.
async fn ensure_attached_function(
    sysdb: &mut SysDb,
    tenant: String,
    database_name: &DatabaseName,
    input_collection_id: CollectionUuid,
    base_source_name: &str,
    cfg: &FoundationConfig,
    endpoint_url: &str,
) -> Result<bool, ServerError> {
    let attached_function_name = foundation_attached_function_name();
    if let Some(function) =
        attached_function_by_name(sysdb, input_collection_id, &attached_function_name).await?
    {
        validate_persisted_function_endpoint(&function, endpoint_url)?;
        tracing::info!(
            attached_function = %attached_function_name,
            %input_collection_id,
            %endpoint_url,
            "foundation function is already attached with the selected endpoint; leaving it as it is"
        );
        return Ok(true);
    }

    let source_kind = source_kind_for_collection_name(base_source_name)?;
    let params = serde_json::json!({
        "endpoint_url": endpoint_url,
        "batch_size": cfg.function_batch_size,
        "source_collection": base_source_name,
        "source_kind": source_kind,
    });
    let output_schema = Schema::new_record_only();
    attached_function_ops::create_attached_function(
        sysdb,
        attached_function_name.clone(),
        cfg.function_name.clone(),
        input_collection_id,
        cfg.wiki_collection.clone(),
        params,
        tenant,
        database_name.as_ref().to_string(),
        cfg.min_records_for_invocation,
        output_schema,
    )
    .await?;

    // The normal create path rejects a concurrent attachment with the same
    // execution mode. Validate after creation as well so this stays safe if a
    // backend instead resolves that race by returning the winner.
    if let Some(function) =
        attached_function_by_name(sysdb, input_collection_id, &attached_function_name).await?
    {
        validate_persisted_function_endpoint(&function, endpoint_url)?;
    }
    Ok(false)
}

/// Find an attached function by name on `collection_id`.
///
/// Whether a create repeats an attachment the input collection already carries
/// is not a question sysdb can be left to answer on its own. A create request
/// carries no attached-function id — sysdb mints one server-side — so sysdb
/// decides by comparing the request against each stored row field by field: the
/// attachment's name, the tenant, the output collection name, the minimum
/// records per invocation, the function the row runs, and the database. A repeat
/// whose six fields all still agree is recognized as one.
///
/// A repeat that differs in any one of them is not. It falls through to a second
/// check, which refuses it with `AlreadyExists` when the stored row runs a
/// function of the same execution mode — asynchronous or synchronous — as the
/// one requested:
///
/// ```text
/// collection already has an attached function with the same execution mode
/// (pre-lock validation): name=foundation_sources_to_wiki,
/// function=http_generate, output_collection=wiki
/// ```
///
/// Three of the six — the output collection name, the minimum records per
/// invocation, and the function name — are read from configuration, so raising
/// the record threshold or renaming the wiki collection is enough to make the
/// next `/init` differ. The refusal reaches a user as a flat "the Chroma API
/// rejected the sync request" and stops onboarding from ever finishing for a
/// workspace that was set up before. Settling the question here, by name, is
/// what delivers the idempotency `/init` advertises however the configuration
/// has since been retuned, and it is what tells the caller the workspace was
/// already set up.
///
/// Matching on the name alone is also what bounds that promise: a workspace
/// whose attachment predates a configuration change keeps the values it was
/// built with, and `/init` reports it as already set up rather than rewriting
/// it. Only the endpoint URL is re-checked, by
/// [`validate_persisted_function_endpoint`]. Changing one of the other five
/// therefore needs the attachment rebuilt, not another `/init`.
///
/// A backend that cannot list attachments answers `None`, which leaves the
/// decision to the create call rather than failing the request: the sqlite
/// backend used for local development does not implement the call.
async fn attached_function_by_name(
    sysdb: &mut SysDb,
    collection_id: CollectionUuid,
    name: &str,
) -> Result<Option<AttachedFunction>, ServerError> {
    listed_attached_functions(sysdb, collection_id)
        .await?
        .into_iter()
        .find(|function| function.name == name)
        .map(AttachedFunction::try_from)
        .transpose()
        .map_err(|error| InvalidPersistedAttachedFunction(error.to_string()).into())
}

/// The functions attached to `collection_id`, as sysdb stores them.
///
/// Invariants:
/// 1. A backend that cannot list attachments answers with an empty list, not an
///    error, so a caller reads "nothing is attached" and keeps going. The
///    sqlite backend used for local development is one such backend.
/// 2. Rows are returned unconverted, so one malformed row fails only the caller
///    that reads it rather than the whole listing.
pub(crate) async fn listed_attached_functions(
    sysdb: &mut SysDb,
    collection_id: CollectionUuid,
) -> Result<Vec<chroma_types::chroma_proto::AttachedFunction>, ServerError> {
    match sysdb.list_attached_functions(collection_id).await {
        Ok(attached) => Ok(attached),
        Err(ListAttachedFunctionsError::NotImplemented) => Ok(Vec::new()),
        Err(error) => Err(ServerError::from(Box::new(error) as Box<dyn ChromaError>)),
    }
}

/// Converts one stored attachment into its typed form.
///
/// A row sysdb cannot round-trip is an internal invariant violation, so this
/// fails rather than dropping the row and reporting a Foundation as holding
/// fewer functions than it does.
pub(crate) fn typed_attached_function(
    function: chroma_types::chroma_proto::AttachedFunction,
) -> Result<AttachedFunction, ServerError> {
    AttachedFunction::try_from(function)
        .map_err(|error| InvalidPersistedAttachedFunction(error.to_string()).into())
}

#[derive(Debug, thiserror::Error)]
#[error("sysdb returned an invalid persisted attached function: {0}")]
struct InvalidPersistedAttachedFunction(String);

impl ChromaError for InvalidPersistedAttachedFunction {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

/// Name of the function that turns a Foundation's source collections into its
/// wiki. One Foundation holds at most one attachment under this name, and its
/// presence is what marks the database as a provisioned Foundation.
pub(crate) fn foundation_attached_function_name() -> String {
    "foundation_sources_to_wiki".to_string()
}

/// Attach the built-in `revision_history` function to the wiki
/// collection so every upsert/delete is archived into the wiki_revisions
/// collection on compaction.
async fn ensure_revision_history_function(
    sysdb: &mut SysDb,
    tenant: String,
    database_name: &DatabaseName,
    wiki_collection_id: CollectionUuid,
    cfg: &FoundationConfig,
) -> Result<(), ServerError> {
    let params = serde_json::json!({
        "version_key": "version",
    });
    let output_schema = Schema::new_record_only();
    attached_function_ops::create_attached_function(
        sysdb,
        "wiki_revision_history".to_string(),
        "revision_history".to_string(),
        wiki_collection_id,
        cfg.wiki_revisions_collection.clone(),
        params,
        tenant,
        database_name.as_ref().to_string(),
        cfg.min_records_for_invocation,
        output_schema,
    )
    .await?;
    Ok(())
}

/// Attach the configured wiki->currents function to the wiki collection so
/// currents are refreshed whenever the wiki advances.
async fn ensure_currents_function(
    sysdb: &mut SysDb,
    tenant: String,
    database_name: &DatabaseName,
    wiki_collection_id: CollectionUuid,
    cfg: &FoundationConfig,
    endpoint_url: &str,
) -> Result<(), ServerError> {
    let attached_function_name = foundation_currents_attached_function_name();
    if let Some(function) =
        attached_function_by_name(sysdb, wiki_collection_id, &attached_function_name).await?
    {
        validate_persisted_function_endpoint(&function, endpoint_url)?;
        tracing::info!(
            attached_function = %attached_function_name,
            %wiki_collection_id,
            %endpoint_url,
            "currents function is already attached with the selected endpoint; leaving it as it is"
        );
        return Ok(());
    }

    // The currents function persists its database into its stored parameters,
    // so a name taken from config here would write currents into the default
    // Foundation no matter which one was initialized.
    let params = serde_json::json!({
        "endpoint_url": endpoint_url,
        "database_name": database_name.as_ref(),
    });
    let output_schema = Schema::new_record_only();
    attached_function_ops::create_attached_function(
        sysdb,
        attached_function_name.clone(),
        cfg.currents_function_name.clone(),
        wiki_collection_id,
        cfg.currents_collection.clone(),
        params,
        tenant,
        database_name.as_ref().to_string(),
        cfg.min_records_for_invocation,
        output_schema,
    )
    .await?;

    if let Some(function) =
        attached_function_by_name(sysdb, wiki_collection_id, &attached_function_name).await?
    {
        validate_persisted_function_endpoint(&function, endpoint_url)?;
    }
    Ok(())
}

fn foundation_currents_attached_function_name() -> String {
    "wiki_currents".to_string()
}

/// Why a Foundation could not be provisioned or read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FoundationInitError {
    #[error("Configured foundation database name is shorter than the 3-character minimum")]
    DatabaseNameTooShort,
    /// The tenant holds no database under this name. Distinct from a database
    /// that exists but was never provisioned, which is reported as an
    /// unprovisioned Foundation rather than an error.
    #[error("foundation '{name}' does not exist")]
    FoundationNotFound { name: String },
}

impl ChromaError for FoundationInitError {
    fn code(&self) -> ErrorCodes {
        match self {
            FoundationInitError::DatabaseNameTooShort => ErrorCodes::InvalidArgument,
            FoundationInitError::FoundationNotFound { .. } => ErrorCodes::NotFound,
        }
    }
}

async fn ensure_currents_collection(
    sysdb: &mut SysDb,
    tenant: String,
    db_name: DatabaseName,
    cfg: &FoundationConfig,
) -> Result<Collection, ServerError> {
    ensure_collection(
        sysdb,
        tenant,
        db_name,
        &cfg.currents_collection,
        None,
        Some(1),
        CollectionEmbeddingFunctions::default(),
    )
    .await
}

/// Ensure a fully indexed Foundation collection: build the Foundation
/// schema (dense + SPLADE sparse indexes, optional embedding functions) and
/// create it via the shared [`create_planned_collection`] core in
/// [`crate::collections`].
#[tracing::instrument(
    name = "ensure_collection",
    skip_all,
    fields(collection = %collection_name, database = %database_name.as_ref()),
    err(Display)
)]
async fn ensure_collection(
    sysdb: &mut SysDb,
    tenant: String,
    database_name: DatabaseName,
    collection_name: &str,
    metadata: Option<Metadata>,
    dimension: Option<i32>,
    embedding_functions: CollectionEmbeddingFunctions,
) -> Result<Collection, ServerError> {
    let schema = foundation_collection_schema(embedding_functions);
    create_planned_collection(
        sysdb,
        tenant,
        database_name,
        collection_name,
        schema,
        metadata,
        dimension,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    #[test]
    fn mock_wiki_selects_the_mock_function_endpoint() {
        let cfg = FoundationConfig {
            function_endpoint_url: Some("https://wiki.example".to_string()),
            mock_wiki_function_endpoint_url: Some("https://mock-wiki.example".to_string()),
            ..FoundationConfig::default()
        };

        assert_eq!(
            configured_function_endpoint_url(&cfg, false),
            Ok("https://wiki.example")
        );
        assert_eq!(
            configured_function_endpoint_url(&cfg, true),
            Ok("https://mock-wiki.example")
        );
    }

    #[test]
    fn mock_wiki_requires_its_own_configured_endpoint() {
        let cfg = FoundationConfig {
            function_endpoint_url: Some("https://wiki.example".to_string()),
            ..FoundationConfig::default()
        };

        assert_eq!(
            configured_function_endpoint_url(&cfg, true),
            Err(MissingFunctionEndpointUrl::MockWiki)
        );
    }

    #[test]
    fn vectorless_sources_are_single_dimension_others_are_1024() {
        // The Drive and Granola connectors upsert a 1-element placeholder
        // vector, so their collections have to be pinned to one dimension.
        assert_eq!(source_dimension("gdrive"), Some(1));
        assert_eq!(source_dimension("gdrive_master"), Some(1));
        assert_eq!(source_dimension("granola"), Some(1));
        assert_eq!(source_dimension("granola_master"), Some(1));
        assert_eq!(source_dimension("notion"), Some(1024));
        // Unknown sources fall back to the default 1024 dims.
        assert_eq!(source_dimension("unknown_source"), Some(1024));
    }

    /// `slack_raw` is the attached function's base input, so its source_kind
    /// must resolve to `slack` — that keeps the generation contract identical
    /// to the old `slack` base and guarantees `ensure_attached_function` won't
    /// error on an unknown source kind.
    #[test]
    fn slack_raw_maps_to_slack_source_kind() {
        assert_eq!(
            source_kind_for_collection_name(SLACK_RAW_COLLECTION_NAME).unwrap(),
            "slack"
        );
    }

    fn attached_function(
        name: &str,
        input_collection_id: CollectionUuid,
        endpoint_url: Option<&str>,
    ) -> chroma_types::AttachedFunction {
        chroma_types::AttachedFunction {
            id: chroma_types::AttachedFunctionUuid::new(),
            name: name.to_string(),
            function_id: uuid::Uuid::new_v4(),
            input_collection_id,
            output_collection_name: "wiki".to_string(),
            output_collection_id: None,
            params: endpoint_url.map(|endpoint_url| {
                serde_json::json!({ "endpoint_url": endpoint_url }).to_string()
            }),
            tenant_id: "tenant".to_string(),
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

    fn sysdb_with(functions: Vec<chroma_types::AttachedFunction>) -> (SysDb, CollectionUuid) {
        let collection_id = CollectionUuid::new();
        let mut test = chroma_sysdb::TestSysDb::new();
        test.set_attached_functions(HashMap::from([(
            collection_id,
            functions
                .into_iter()
                .map(|function| chroma_types::AttachedFunction {
                    input_collection_id: collection_id,
                    ..function
                })
                .collect(),
        )]));
        (SysDb::Test(test), collection_id)
    }

    #[test]
    fn persisted_endpoint_accepts_an_equivalent_selected_endpoint() {
        let function = attached_function(
            &foundation_attached_function_name(),
            CollectionUuid::new(),
            Some("https://wiki.example/"),
        );

        assert_eq!(
            validate_persisted_function_endpoint(&function, "https://wiki.example"),
            Ok(())
        );
    }

    #[test]
    fn persisted_endpoint_rejects_conflicts_and_invalid_params() {
        let function = attached_function(
            &foundation_attached_function_name(),
            CollectionUuid::new(),
            Some("https://wiki.example"),
        );
        let conflict = validate_persisted_function_endpoint(&function, "https://mock-wiki.example")
            .expect_err("a different persisted endpoint must reject init");
        assert_eq!(conflict.code(), ErrorCodes::FailedPrecondition);
        assert!(matches!(
            conflict,
            PersistedFunctionEndpointError::Conflict { .. }
        ));

        let function = attached_function(
            &foundation_attached_function_name(),
            CollectionUuid::new(),
            None,
        );
        assert!(matches!(
            validate_persisted_function_endpoint(&function, "https://wiki.example"),
            Err(PersistedFunctionEndpointError::MissingOrInvalid { .. })
        ));
    }

    #[tokio::test]
    async fn existing_sources_function_rejects_a_different_selected_endpoint() {
        let (mut sysdb, collection_id) = sysdb_with(vec![attached_function(
            &foundation_attached_function_name(),
            CollectionUuid::new(),
            Some("https://wiki.example"),
        )]);

        let error = match ensure_attached_function(
            &mut sysdb,
            "tenant".to_string(),
            &DatabaseName::new("FOUNDATION").expect("valid database name"),
            collection_id,
            SLACK_RAW_COLLECTION_NAME,
            &FoundationConfig::default(),
            "https://mock-wiki.example",
        )
        .await
        {
            Ok(_) => panic!("a conflicting persisted endpoint must reject init"),
            Err(error) => error,
        };

        assert_eq!(error.0.code(), ErrorCodes::FailedPrecondition);
    }

    #[tokio::test]
    async fn existing_currents_function_rejects_a_different_selected_endpoint() {
        let (mut sysdb, collection_id) = sysdb_with(vec![attached_function(
            &foundation_currents_attached_function_name(),
            CollectionUuid::new(),
            Some("https://wiki.example"),
        )]);

        let error = match ensure_currents_function(
            &mut sysdb,
            "tenant".to_string(),
            &DatabaseName::new("FOUNDATION").expect("valid database name"),
            collection_id,
            &FoundationConfig::default(),
            "https://mock-wiki.example",
        )
        .await
        {
            Ok(_) => panic!("a conflicting persisted endpoint must reject init"),
            Err(error) => error,
        };

        assert_eq!(error.0.code(), ErrorCodes::FailedPrecondition);
    }

    /// `ServerError` carries no `Debug`, so `unwrap` is unavailable here.
    async fn attached_by_name(
        sysdb: &mut SysDb,
        collection_id: CollectionUuid,
    ) -> Option<AttachedFunction> {
        match attached_function_by_name(sysdb, collection_id, &foundation_attached_function_name())
            .await
        {
            Ok(found) => found,
            Err(_) => panic!("listing attached functions should succeed against the test sysdb"),
        }
    }

    /// The case that broke onboarding: a workspace set up earlier already has
    /// the function, and `/init` must leave it alone rather than attempting an
    /// attach that sysdb refuses with `AlreadyExists`.
    #[tokio::test]
    async fn an_existing_attachment_is_recognized() {
        let (mut sysdb, collection_id) = sysdb_with(vec![attached_function(
            &foundation_attached_function_name(),
            CollectionUuid::new(),
            Some("https://wiki.example"),
        )]);

        assert!(attached_by_name(&mut sysdb, collection_id).await.is_some());
    }

    /// A first-time workspace, and a collection carrying somebody else's
    /// function, both still need the attach to run.
    #[tokio::test]
    async fn anything_else_still_needs_attaching() {
        let (mut sysdb, empty) = sysdb_with(vec![]);
        assert!(attached_by_name(&mut sysdb, empty).await.is_none());

        let (mut sysdb, other) = sysdb_with(vec![attached_function(
            "revision_history",
            CollectionUuid::new(),
            None,
        )]);
        assert!(
            attached_by_name(&mut sysdb, other).await.is_none(),
            "matching on name only — a different function must not be mistaken for ours"
        );
    }
}
