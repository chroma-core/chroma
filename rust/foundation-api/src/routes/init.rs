use super::init_schema::{
    foundation_collection_schema, qwen_embedding_function, splade_embedding_function,
    CollectionEmbeddingFunctions,
};
use super::whoami::whoami_and_authorize;
use crate::collections::{create_planned_collection, ensure_database, ensure_slack_raw_collection};
use crate::{
    auth::AuthzAction, config::FoundationConfig, errors::ServerError, server::FoundationApiServer,
};
use axum::{extract::State, http::HeaderMap, Json};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_types::{
    Collection, CollectionUuid, DatabaseName, ListAttachedFunctionsError, Metadata, MetadataValue,
    Schema, CHROMA_GROUP_CHUNK_SIBLINGS_KEY, SLACK_RAW_COLLECTION_NAME,
};
use frontend_core::{
    attached_function_ops,
    foundation::{
        foundation_source_for_collection_name, source_kind_for_collection_name,
        FoundationSourceKindError,
    },
};
use serde::Serialize;
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

/// `POST /api/init` — idempotent bootstrap for a team's Foundation
/// workspace. Ensures the configured Foundation database and the wiki +
/// wiki_revisions collections (names overridable via
/// `CHROMA_FOUNDATION__*` env vars) exist in the tenant resolved from the
/// auth context. Safe to call repeatedly.
#[tracing::instrument(name = "foundation_init", skip_all, err(Display))]
pub async fn foundation_init(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
) -> Result<Json<FoundationInitResponse>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::InitFoundation).await?;
    let tenant = identity.tenant;
    let user_id = identity.user_id;
    tracing::info!(
        tenant = %tenant,
        user_id = %user_id,
        database = %server.config.foundation.database_name,
        "foundation init starting"
    );

    let _guard =
        server.scorecard_request(&["op:foundation_init", &format!("tenant:{}", tenant)])?;

    let foundation_cfg = &server.config.foundation;
    let db_name = DatabaseName::new(&foundation_cfg.database_name)
        .ok_or(FoundationInitError::DatabaseNameTooShort)?;
    let indexed_sources =
        indexed_source_descriptors(&foundation_cfg.indexed_source_collections)?;

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
        wiki.collection_id,
        foundation_cfg,
    )
    .await?;
    ensure_currents_function(
        &mut sysdb,
        tenant.clone(),
        wiki.collection_id,
        foundation_cfg,
    )
    .await?;

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
    let mut indexed_source_collection_ids = Vec::new();
    for source_descriptor in indexed_sources {
        let source = ensure_collection(
            &mut sysdb,
            tenant.clone(),
            db_name.clone(),
            source_descriptor.collection_name,
            Some(group_chunk_siblings_metadata()),
            Some(source_descriptor.dimension),
            CollectionEmbeddingFunctions::default(),
        )
        .await?;
        indexed_source_collection_ids.push(source.collection_id);
        source_collection_ids.insert(
            source_descriptor.collection_name.to_string(),
            source.collection_id.to_string(),
        );
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
        slack_raw.collection_id,
        SLACK_RAW_COLLECTION_NAME,
        foundation_cfg,
    )
    .await?;

    // Add the indexed sources and the per-user coding-agent traces collection
    // as extra inputs to the same sources->wiki function, so they flow into the
    // shared wiki output alongside slack_raw.
    for source_collection_id in &indexed_source_collection_ids {
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
        num_indexed_source_collections = source_collection_ids.len(),
        "foundation init complete"
    );

    Ok(Json(FoundationInitResponse {
        tenant,
        user_id,
        database: foundation_cfg.database_name.clone(),
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
    }))
}

struct IndexedSourceDescriptor<'a> {
    collection_name: &'a str,
    dimension: i32,
}

/// Validate all configured sources before `/init` makes any provisioning
/// writes, and retain the source-specific collection settings for creation.
fn indexed_source_descriptors(
    source_names: &[String],
) -> Result<Vec<IndexedSourceDescriptor<'_>>, FoundationSourceKindError> {
    source_names
        .iter()
        .map(|collection_name| {
            let source = foundation_source_for_collection_name(collection_name)?;
            Ok(IndexedSourceDescriptor {
                collection_name,
                dimension: source.dimension(),
            })
        })
        .collect()
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

/// Raised when `/init` needs the attached-function endpoint URL but the
/// deployment never configured `foundation.function_endpoint_url`. Surfaced
/// as a 500 so a misconfigured deploy fails loudly instead of attaching the
/// function with a missing/placeholder endpoint.
#[derive(Debug, thiserror::Error)]
#[error("foundation.function_endpoint_url is not configured")]
struct MissingFunctionEndpointUrl;

impl ChromaError for MissingFunctionEndpointUrl {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

/// Idempotently create the shared foundation function on the base source
/// collection. Additional source collections are attached later via
/// `add_input()`. Params carry the modal endpoint and the base source kind,
/// matching the existing Foundation function contract.
///
/// `/init` is safe to call repeatedly: an attachment that is already there is
/// left alone. Checking here is what makes that true — see
/// [`foundation_function_already_attached`] for why the layer below cannot.
///
/// Returns whether the attachment was already present, which is what tells the
/// caller this workspace had been set up before.
async fn ensure_attached_function(
    sysdb: &mut SysDb,
    tenant: String,
    input_collection_id: CollectionUuid,
    base_source_name: &str,
    cfg: &FoundationConfig,
) -> Result<bool, ServerError> {
    if foundation_function_already_attached(sysdb, input_collection_id).await? {
        tracing::info!(
            attached_function = %foundation_attached_function_name(),
            %input_collection_id,
            "foundation function is already attached; leaving it as it is"
        );
        return Ok(true);
    }

    let endpoint_url = cfg
        .function_endpoint_url
        .as_ref()
        .ok_or(MissingFunctionEndpointUrl)?;
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
        foundation_attached_function_name(),
        cfg.function_name.clone(),
        input_collection_id,
        cfg.wiki_collection.clone(),
        params,
        tenant,
        cfg.database_name.clone(),
        cfg.min_records_for_invocation,
        output_schema,
    )
    .await?;
    Ok(false)
}

/// Whether the foundation function is already attached to `collection_id`.
///
/// Attaching cannot be left to fail harmlessly. sysdb decides whether a create
/// is a repeat by comparing the *attached-function id*, and `/init` mints a
/// fresh one on every call, so a second `/init` never matches. It falls through
/// to the execution-mode check instead and is refused with `AlreadyExists`:
///
/// ```text
/// collection already has an attached function with the same execution mode:
/// name=foundation_sources_to_wiki, function=http_generate, output_collection=wiki
/// ```
///
/// That reaches a user as a flat "the Chroma API rejected the sync request",
/// and it means onboarding can never finish for a workspace that was set up
/// before — which is every returning user. Looking the attachment up by name
/// first is what actually delivers the idempotency `/init` advertises.
///
/// A backend that cannot list attachments answers `false`, so this reduces to
/// the previous behaviour rather than failing: the sqlite backend used for
/// local development does not implement the call.
async fn foundation_function_already_attached(
    sysdb: &mut SysDb,
    collection_id: CollectionUuid,
) -> Result<bool, ServerError> {
    let name = foundation_attached_function_name();
    match sysdb.list_attached_functions(collection_id).await {
        Ok(attached) => Ok(attached.iter().any(|function| function.name == name)),
        Err(ListAttachedFunctionsError::NotImplemented) => Ok(false),
        Err(error) => Err(ServerError::from(Box::new(error) as Box<dyn ChromaError>)),
    }
}

fn foundation_attached_function_name() -> String {
    "foundation_sources_to_wiki".to_string()
}

/// Attach the built-in `revision_history` function to the wiki
/// collection so every upsert/delete is archived into the wiki_revisions
/// collection on compaction.
async fn ensure_revision_history_function(
    sysdb: &mut SysDb,
    tenant: String,
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
        cfg.database_name.clone(),
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
    wiki_collection_id: CollectionUuid,
    cfg: &FoundationConfig,
) -> Result<(), ServerError> {
    let endpoint_url = cfg
        .function_endpoint_url
        .as_ref()
        .ok_or(MissingFunctionEndpointUrl)?;
    let params = serde_json::json!({
        "endpoint_url": endpoint_url,
        "database_name": cfg.database_name,
    });
    let output_schema = Schema::new_record_only();
    attached_function_ops::create_attached_function(
        sysdb,
        foundation_currents_attached_function_name(),
        cfg.currents_function_name.clone(),
        wiki_collection_id,
        cfg.currents_collection.clone(),
        params,
        tenant,
        cfg.database_name.clone(),
        cfg.min_records_for_invocation,
        output_schema,
    )
    .await?;
    Ok(())
}

fn foundation_currents_attached_function_name() -> String {
    "wiki_currents".to_string()
}

#[derive(Debug, thiserror::Error)]
enum FoundationInitError {
    #[error("Configured foundation database name is shorter than the 3-character minimum")]
    DatabaseNameTooShort,
}

impl ChromaError for FoundationInitError {
    fn code(&self) -> ErrorCodes {
        match self {
            FoundationInitError::DatabaseNameTooShort => ErrorCodes::InvalidArgument,
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
    use crate::config::FoundationApiConfig;
    use chroma_sysdb::TestSysDb;
    use chroma_system::System;
    use std::sync::Arc;
    use std::time::SystemTime;

    #[test]
    fn init_source_descriptors_match_worker_source_mapping() {
        let source_names = [
            "slack_master",
            "notion_master",
            "gdrive_master",
            "google_drive_master",
            "coding_sessions",
            "agent_sessions_user",
            "granola_master",
        ]
        .map(str::to_string);
        let descriptors = indexed_source_descriptors(&source_names).unwrap();

        for descriptor in &descriptors {
            assert!(source_kind_for_collection_name(descriptor.collection_name).is_ok());
        }
        assert_eq!(
            descriptors
                .iter()
                .map(|descriptor| descriptor.dimension)
                .collect::<Vec<_>>(),
            vec![1024, 1024, 1, 1, 1024, 1024, 1024]
        );
    }

    #[tokio::test]
    async fn unsupported_source_fails_before_provisioning() {
        let mut config = FoundationApiConfig::default();
        config.foundation.indexed_source_collections = vec!["custom_docs".to_string()];
        let server = FoundationApiServer::new(
            config,
            Arc::new(()),
            SysDb::Test(TestSysDb::new()),
            vec![],
            System::new(),
        );

        // TestSysDb panics on database creation, so this error also proves
        // source validation precedes the first provisioning write.
        let error = match foundation_init(HeaderMap::new(), State(server)).await {
            Ok(_) => panic!("unsupported source configuration should fail"),
            Err(error) => error,
        };

        assert_eq!(error.0.code(), ErrorCodes::InvalidArgument);
        assert_eq!(
            error.to_string(),
            "unknown foundation source collection: custom_docs"
        );
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
    ) -> chroma_types::AttachedFunction {
        chroma_types::AttachedFunction {
            id: chroma_types::AttachedFunctionUuid::new(),
            name: name.to_string(),
            function_id: uuid::Uuid::new_v4(),
            input_collection_id,
            output_collection_name: "wiki".to_string(),
            output_collection_id: None,
            params: None,
            tenant_id: "tenant".to_string(),
            database_id: "database".to_string(),
            last_run: None,
            completion_offset: 0,
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

    /// `ServerError` carries no `Debug`, so `unwrap` is unavailable here.
    async fn already_attached(sysdb: &mut SysDb, collection_id: CollectionUuid) -> bool {
        match foundation_function_already_attached(sysdb, collection_id).await {
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
        )]);

        assert!(already_attached(&mut sysdb, collection_id).await);
    }

    /// A first-time workspace, and a collection carrying somebody else's
    /// function, both still need the attach to run.
    #[tokio::test]
    async fn anything_else_still_needs_attaching() {
        let (mut sysdb, empty) = sysdb_with(vec![]);
        assert!(!already_attached(&mut sysdb, empty).await);

        let (mut sysdb, other) = sysdb_with(vec![attached_function(
            "revision_history",
            CollectionUuid::new(),
        )]);
        assert!(
            !already_attached(&mut sysdb, other).await,
            "matching on name only — a different function must not be mistaken for ours"
        );
    }
}
