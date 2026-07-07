//! Idempotent database/collection creation against sysdb.
//!
//! This is the schema-agnostic machinery `/init` builds on: plan a
//! distributed-mode collection with the shared `frontend_core` planner, hand
//! it to `SysDb::create_collection` with `GET_OR_CREATE`, and retry transient
//! failures. It lives at the crate level (not under `routes`) because it is
//! not route logic, and it is exported so hosted-chroma's sync service can
//! create collections — in particular `slack_raw` — identically to `/init`.
//!
//! Everything here depends only on `frontend-core`, `chroma-sysdb`, and
//! `chroma-types`, so the module can later be promoted into `frontend-core`
//! (next to `collection_ops`, which already hosts the pure planner) without
//! reworking call sites.

use crate::errors::ServerError;
use chroma_sysdb::SysDb;
use chroma_types::{
    slack_raw_schema, Collection, CreateDatabaseError, DatabaseName, KnnIndex, Metadata, Schema,
};
use frontend_core::{
    collection_ops::{
        plan_create_collection, supported_segment_types, ExecutorKind, TenantFeatureFlags,
    },
    retry::retry_transient,
};
use uuid::Uuid;

/// SysDb's `create_collection` takes a `get_or_create: bool`. When true, an
/// existing collection with the same (tenant, database, name) is returned
/// instead of failing with `AlreadyExists` — atomic idempotency in one round
/// trip, so we don't need the try-then-fallback dance we use for databases.
const GET_OR_CREATE: bool = true;

/// Idempotently ensure a database exists and return its id.
#[tracing::instrument(
    name = "ensure_database",
    skip_all,
    fields(database = %database_name.as_ref(), tenant = %tenant),
    err(Display)
)]
pub async fn ensure_database(
    sysdb: &mut SysDb,
    database_name: DatabaseName,
    tenant: String,
) -> Result<Uuid, ServerError> {
    // Generate the id once so retries don't churn through fresh UUIDs; the
    // create is keyed on the (tenant, name) pair and is idempotent — an
    // `AlreadyExists` from a previous/concurrent init is the success path.
    let database_id = Uuid::new_v4();
    let created = match retry_transient(|| {
        let mut sysdb = sysdb.clone();
        let database_name = database_name.clone();
        let tenant = tenant.clone();
        async move {
            sysdb
                .create_database(database_id, database_name, tenant)
                .await
        }
    })
    .await
    {
        Ok(_) => true,
        Err(CreateDatabaseError::AlreadyExists(_)) => false,
        Err(e) => return Err(e.into()),
    };

    let db = retry_transient(|| {
        let mut sysdb = sysdb.clone();
        let database_name = database_name.clone();
        let tenant = tenant.clone();
        async move { sysdb.get_database(database_name, tenant).await }
    })
    .await?;

    tracing::info!(database = %database_name.as_ref(), database_id = %db.id, created, "ensured foundation database");
    Ok(db.id)
}

/// Ensure the `slack_raw` collection. Uses the shared hybrid schema: metadata
/// inverted indexes enabled (records are filterable by channel/team/thread/
/// op), FTS/dense/sparse indexes disabled, no embedding function, and no
/// pinned dimension — documents are stored verbatim and rendering/embedding
/// is deferred to the attached function. The schema spec is shared with
/// hosted-chroma's sync service via [`chroma_types::slack_raw_schema`].
#[tracing::instrument(
    name = "ensure_slack_raw_collection",
    skip_all,
    fields(collection = %collection_name, database = %database_name.as_ref()),
    err(Display)
)]
pub async fn ensure_slack_raw_collection(
    sysdb: &mut SysDb,
    tenant: String,
    database_name: DatabaseName,
    collection_name: &str,
    metadata: Option<Metadata>,
) -> Result<Collection, ServerError> {
    create_planned_collection(
        sysdb,
        tenant,
        database_name,
        collection_name,
        slack_raw_schema()?,
        metadata,
        // No dense vectors are written, so there is no dimension to pin
        // (mirrors currents / wiki_revisions).
        None,
    )
    .await
}

/// Shared core for the `ensure_*_collection` helpers: plan a fresh
/// distributed-mode collection from the given `schema` and hand it to sysdb.
/// `GET_OR_CREATE` keeps it idempotent in a single round trip, so a transient
/// sysdb failure is safe to retry; the plan (collection id + segments +
/// config) is fixed up front and reused across attempts.
pub async fn create_planned_collection(
    sysdb: &mut SysDb,
    tenant: String,
    database_name: DatabaseName,
    collection_name: &str,
    schema: Schema,
    metadata: Option<Metadata>,
    dimension: Option<i32>,
) -> Result<Collection, ServerError> {
    let plan = plan_create_collection(
        None,
        Some(schema),
        ExecutorKind::Distributed,
        &supported_segment_types(ExecutorKind::Distributed),
        true,
        KnnIndex::Spann,
        TenantFeatureFlags::default(),
    )?;
    let collection_id = plan.collection_id;
    let collection = retry_transient(|| {
        let mut sysdb = sysdb.clone();
        let tenant = tenant.clone();
        let database_name = database_name.clone();
        let collection_name = collection_name.to_string();
        let segments = plan.segments.clone();
        let configuration = plan.configuration.clone();
        let schema = plan.schema.clone();
        let metadata = metadata.clone();
        async move {
            sysdb
                .create_collection(
                    tenant,
                    database_name,
                    collection_id,
                    collection_name,
                    segments,
                    configuration,
                    schema,
                    metadata,
                    dimension,
                    GET_OR_CREATE,
                )
                .await
        }
    })
    .await?;

    tracing::info!(
        collection = %collection_name,
        collection_id = %collection.collection_id,
        "ensured foundation collection"
    );
    Ok(collection)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `slack_raw` is created through the same planner path
    /// [`create_planned_collection`] uses. Assert the hybrid survives
    /// `reconcile_schema_and_config`: metadata inverted indexes stay enabled
    /// while FTS and the dense/sparse vector indexes stay disabled.
    #[test]
    fn slack_raw_plan_indexes_metadata_not_text() {
        let schema = slack_raw_schema().expect("static schema construction must succeed");
        let plan = plan_create_collection(
            None,
            Some(schema),
            ExecutorKind::Distributed,
            &supported_segment_types(ExecutorKind::Distributed),
            true,
            KnnIndex::Spann,
            TenantFeatureFlags::default(),
        )
        .expect("planning the slack_raw schema must succeed");

        let reconciled = plan
            .schema
            .as_ref()
            .expect("plan must carry a reconciled schema when enable_schema=true");

        // Metadata inverted indexes survive reconciliation.
        let string = reconciled
            .defaults
            .string
            .as_ref()
            .expect("schema defaults must carry a string value type");
        assert!(
            string.string_inverted_index.as_ref().unwrap().enabled,
            "slack_raw must keep its string metadata index after planning"
        );
        let int = reconciled
            .defaults
            .int
            .as_ref()
            .expect("schema defaults must carry an int value type");
        assert!(
            int.int_inverted_index.as_ref().unwrap().enabled,
            "slack_raw must keep its int metadata index after planning"
        );
        let float = reconciled
            .defaults
            .float
            .as_ref()
            .expect("schema defaults must carry a float value type");
        assert!(
            float.float_inverted_index.as_ref().unwrap().enabled,
            "slack_raw must keep its float metadata index after planning"
        );
        let boolean = reconciled
            .defaults
            .boolean
            .as_ref()
            .expect("schema defaults must carry a bool value type");
        assert!(
            boolean.bool_inverted_index.as_ref().unwrap().enabled,
            "slack_raw must keep its bool metadata index after planning"
        );

        // Text/vector indexing stays disabled after reconciliation.
        assert!(
            !reconciled.is_sparse_index_enabled(),
            "slack_raw must have no sparse vector index after planning"
        );
        assert!(
            !reconciled.is_fts_enabled(),
            "slack_raw must have no FTS index after planning"
        );
        let float_list = reconciled
            .defaults
            .float_list
            .as_ref()
            .expect("schema defaults must carry a dense vector index entry");
        assert!(
            !float_list.vector_index.as_ref().unwrap().enabled,
            "slack_raw must have no dense vector index after planning"
        );
    }
}
