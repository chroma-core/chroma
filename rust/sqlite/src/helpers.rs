use crate::table::{EmbeddingMetadataArray, MetadataTable};
use chroma_error::{ChromaError, WrappedSqlxError};
use chroma_types::{CollectionUuid, Metadata, MetadataValue, UpdateMetadata};
use sea_query::{Expr, Iden, InsertStatement, OnConflict, SimpleExpr, SqliteQueryBuilder};
use sea_query::{Nullable, Query};
use sea_query_binder::SqlxBinder;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("Error constructing query: {0}")]
    QueryError(#[from] sea_query::error::Error),
    #[error("Error executing query: {0}")]
    SqlxError(#[from] WrappedSqlxError),
}

impl ChromaError for MetadataError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            MetadataError::QueryError(_) => chroma_error::ErrorCodes::Internal,
            MetadataError::SqlxError(e) => e.code(),
        }
    }
}

fn construct_upsert_metadata_stmt<
    Table: MetadataTable + Iden + 'static,
    Id: Into<SimpleExpr> + Clone,
>(
    id: Id,
    metadata: Metadata,
) -> Result<InsertStatement, sea_query::error::Error> {
    let mut stmt = Query::insert();
    stmt.into_table(Table::table_name())
        .columns([
            Table::id_column(),
            Table::key_column(),
            Table::str_value_column(),
            Table::int_value_column(),
            Table::float_value_column(),
            Table::bool_value_column(),
        ])
        .on_conflict(
            OnConflict::columns([Table::id_column(), Table::key_column()])
                .update_columns([
                    Table::str_value_column(),
                    Table::int_value_column(),
                    Table::float_value_column(),
                    Table::bool_value_column(),
                ])
                .to_owned(),
        );
    for (key, val) in metadata {
        stmt.values(match val {
            MetadataValue::Bool(b) => [
                id.clone().into(),
                key.into(),
                String::null().into(),
                i32::null().into(),
                f32::null().into(),
                b.into(),
            ],
            MetadataValue::Int(i) => [
                id.clone().into(),
                key.into(),
                String::null().into(),
                i.into(),
                f32::null().into(),
                bool::null().into(),
            ],
            MetadataValue::Float(f) => [
                id.clone().into(),
                key.into(),
                String::null().into(),
                i32::null().into(),
                f.into(),
                bool::null().into(),
            ],
            MetadataValue::Str(s) => [
                id.clone().into(),
                key.into(),
                s.into(),
                i32::null().into(),
                f32::null().into(),
                bool::null().into(),
            ],
            MetadataValue::SparseVector(_) => {
                todo!("Sparse vector is not yet supported for local")
            }
            // Array types are routed to the separate embedding_metadata_array
            // table and should never appear here.
            MetadataValue::BoolArray(_)
            | MetadataValue::IntArray(_)
            | MetadataValue::FloatArray(_)
            | MetadataValue::StringArray(_) => {
                unreachable!("Array metadata values are written to embedding_metadata_array")
            }
        })?;
    }
    Ok(stmt)
}

pub async fn update_metadata<
    Table: MetadataTable + Iden + 'static,
    Id: Into<SimpleExpr> + Clone,
    C,
>(
    conn: &mut C,
    id: Id,
    metadata: UpdateMetadata,
) -> Result<(), MetadataError>
where
    for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
{
    let mut deleted_keys = Vec::new();
    let mut metadata_not_null = HashMap::new();
    for (key, value) in metadata {
        match (&value).try_into() {
            Ok(val) => {
                metadata_not_null.insert(key, val);
            }
            Err(_) => deleted_keys.push(key),
        }
    }
    if !deleted_keys.is_empty() {
        let (sql, values) = Query::delete()
            .from_table(Table::table_name())
            .and_where(
                Expr::col(Table::id_column())
                    .eq(id.clone())
                    .and(Expr::col(Table::key_column()).is_in(deleted_keys)),
            )
            .to_owned()
            .build_sqlx(SqliteQueryBuilder);

        sqlx::query_with(&sql, values)
            .execute(&mut *conn)
            .await
            .map_err(WrappedSqlxError)?;
    }
    if !metadata_not_null.is_empty() {
        let (sql, values) = construct_upsert_metadata_stmt::<Table, Id>(id, metadata_not_null)?
            .build_sqlx(SqliteQueryBuilder);

        sqlx::query_with(&sql, values)
            .execute(conn)
            .await
            .map_err(WrappedSqlxError)?;
    }
    Ok(())
}

pub async fn delete_metadata<
    Table: MetadataTable + Iden + 'static,
    Id: Into<SimpleExpr> + Clone,
    C,
>(
    conn: &mut C,
    id: Id,
) -> Result<(), MetadataError>
where
    for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
{
    let (sql, values) = Query::delete()
        .from_table(Table::table_name())
        .and_where(Expr::col(Table::id_column()).eq(id))
        .to_owned()
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(conn)
        .await
        .map_err(WrappedSqlxError)?;

    Ok(())
}

// ── Array metadata helpers (embedding_metadata_array table) ──────────

/// Upsert exploded array metadata values to `embedding_metadata_array`.
///
/// For each array `MetadataValue`, every element is inserted as a separate
/// row.  Before inserting, any existing rows for the same `(id, key)` pairs
/// are deleted so that updates replace old arrays.
///
/// Scalar values in `metadata` are silently skipped — they belong in the
/// regular `embedding_metadata` table.
pub async fn upsert_array_metadata<Id: Into<SimpleExpr> + Clone, C>(
    conn: &mut C,
    id: Id,
    metadata: &Metadata,
) -> Result<(), MetadataError>
where
    for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
{
    let array_keys: Vec<&String> = metadata
        .iter()
        .filter(|(_, v)| {
            matches!(
                v,
                MetadataValue::BoolArray(_)
                    | MetadataValue::IntArray(_)
                    | MetadataValue::FloatArray(_)
                    | MetadataValue::StringArray(_)
            )
        })
        .map(|(k, _)| k)
        .collect();

    if array_keys.is_empty() {
        return Ok(());
    }

    // Delete old exploded rows for these keys.
    let (del_sql, del_vals) = Query::delete()
        .from_table(EmbeddingMetadataArray::Table)
        .and_where(
            Expr::col(EmbeddingMetadataArray::Id).eq(id.clone()).and(
                Expr::col(EmbeddingMetadataArray::Key)
                    .is_in(array_keys.iter().map(|k| k.as_str()).collect::<Vec<_>>()),
            ),
        )
        .to_owned()
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&del_sql, del_vals)
        .execute(&mut *conn)
        .await
        .map_err(WrappedSqlxError)?;

    let mut stmt = Query::insert();
    stmt.into_table(EmbeddingMetadataArray::Table).columns([
        EmbeddingMetadataArray::Id,
        EmbeddingMetadataArray::Key,
        EmbeddingMetadataArray::StringValue,
        EmbeddingMetadataArray::IntValue,
        EmbeddingMetadataArray::FloatValue,
        EmbeddingMetadataArray::BoolValue,
    ]);

    let mut has_rows = false;
    for (key, val) in metadata {
        match val {
            MetadataValue::BoolArray(arr) => {
                for b in arr {
                    stmt.values([
                        id.clone().into(),
                        key.clone().into(),
                        String::null().into(),
                        i32::null().into(),
                        f32::null().into(),
                        (*b).into(),
                    ])?;
                    has_rows = true;
                }
            }
            MetadataValue::IntArray(arr) => {
                for i in arr {
                    stmt.values([
                        id.clone().into(),
                        key.clone().into(),
                        String::null().into(),
                        (*i).into(),
                        f32::null().into(),
                        bool::null().into(),
                    ])?;
                    has_rows = true;
                }
            }
            MetadataValue::FloatArray(arr) => {
                for f in arr {
                    stmt.values([
                        id.clone().into(),
                        key.clone().into(),
                        String::null().into(),
                        i32::null().into(),
                        (*f).into(),
                        bool::null().into(),
                    ])?;
                    has_rows = true;
                }
            }
            MetadataValue::StringArray(arr) => {
                for s in arr {
                    stmt.values([
                        id.clone().into(),
                        key.clone().into(),
                        s.clone().into(),
                        i32::null().into(),
                        f32::null().into(),
                        bool::null().into(),
                    ])?;
                    has_rows = true;
                }
            }
            // Skip scalars — they go in the regular metadata table.
            _ => {}
        }
    }

    if has_rows {
        let (sql, values) = stmt.build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&sql, values)
            .execute(conn)
            .await
            .map_err(WrappedSqlxError)?;
    }

    Ok(())
}

/// Delete all array metadata rows for a given embedding id.
pub async fn delete_array_metadata<Id: Into<SimpleExpr> + Clone, C>(
    conn: &mut C,
    id: Id,
) -> Result<(), MetadataError>
where
    for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
{
    let (sql, values) = Query::delete()
        .from_table(EmbeddingMetadataArray::Table)
        .and_where(Expr::col(EmbeddingMetadataArray::Id).eq(id))
        .to_owned()
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(conn)
        .await
        .map_err(WrappedSqlxError)?;

    Ok(())
}

/// Delete array metadata rows for specific keys that are being set to None.
pub async fn delete_array_metadata_keys<Id: Into<SimpleExpr> + Clone, C>(
    conn: &mut C,
    id: Id,
    keys: &[String],
) -> Result<(), MetadataError>
where
    for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
{
    if keys.is_empty() {
        return Ok(());
    }
    let (sql, values) = Query::delete()
        .from_table(EmbeddingMetadataArray::Table)
        .and_where(
            Expr::col(EmbeddingMetadataArray::Id)
                .eq(id)
                .and(Expr::col(EmbeddingMetadataArray::Key).is_in(keys.to_vec())),
        )
        .to_owned()
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(conn)
        .await
        .map_err(WrappedSqlxError)?;

    Ok(())
}

pub fn get_embeddings_queue_topic_name(
    log_tenant: &str,
    log_topic_namespace: &str,
    collection_id: CollectionUuid,
) -> String {
    format!(
        "persistent://{}/{}/{}",
        log_tenant, log_topic_namespace, collection_id
    )
}
